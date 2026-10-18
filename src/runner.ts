import {
  defaultWorkflowRunnerTimers,
  runAttempt,
  toRegistration,
  wait,
  type AttemptOutcome,
} from "./attempts.js";
import { createBuiltInExecutors } from "./builtins.js";
import { digestExecutableWorkflow, findCheckpointProblem } from "./checkpoint.js";
import {
  type ExecutableWorkflow,
  type ExecutableWorkflowEdge,
  type ExecutableWorkflowNode,
  type ExecutableWorkflowPort,
  type WorkflowExecutorRegistration,
  type WorkflowNodeResult,
  type WorkflowRunCheckpoint,
  type WorkflowRunError,
  type WorkflowRunEvent,
  type WorkflowRunner,
  type WorkflowRunnerOptions,
  workflowRunCheckpointFormat,
} from "./types.js";
import { validateExecutableWorkflow } from "./validation.js";

const hasOwn = (value: object, key: PropertyKey): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

function compareIds(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function fail(
  code: WorkflowRunError["code"],
  message: string,
  options: Omit<WorkflowRunError, "code" | "message"> = {},
): WorkflowRunError {
  return { code, message, ...options };
}

function isMultiValuePort(port: ExecutableWorkflowPort): boolean {
  if (port.cardinality === undefined) {
    return false;
  }
  const max = port.cardinality.max;
  return max === undefined || max === null || max > 1;
}

function runtimeMinimum(port: ExecutableWorkflowPort): number {
  if (port.cardinality?.min !== undefined) {
    return port.cardinality.min;
  }
  if (port.optional === true || hasOwn(port, "defaultValue")) {
    return 0;
  }
  return 1;
}

type InputBuildResult = {
  inputs: Record<string, unknown>;
  activeEdgeCount: number;
  error?: WorkflowRunError;
};

function buildInputs(
  node: ExecutableWorkflowNode,
  incomingEdges: readonly ExecutableWorkflowEdge[],
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
): InputBuildResult {
  const valuesByPort = new Map<string, unknown[]>();
  let activeEdgeCount = 0;

  for (const edge of [...incomingEdges].sort((left, right) => compareIds(left.id, right.id))) {
    const sourceResult = nodeResults[edge.sourceNodeId];
    if (sourceResult?.status !== "succeeded" || !hasOwn(sourceResult.outputs, edge.sourcePortId)) {
      continue;
    }
    const values = valuesByPort.get(edge.targetPortId) ?? [];
    values.push(sourceResult.outputs[edge.sourcePortId]);
    valuesByPort.set(edge.targetPortId, values);
    activeEdgeCount += 1;
  }

  const inputs: Record<string, unknown> = {};
  for (const port of node.inputs ?? []) {
    const values = valuesByPort.get(port.id) ?? [];
    const hasDefault = hasOwn(port, "defaultValue");
    const availableCount = values.length + (values.length === 0 && hasDefault ? 1 : 0);
    const minimum = runtimeMinimum(port);
    if (availableCount < minimum) {
      return {
        inputs,
        activeEdgeCount,
        error: fail(
          "input-not-ready",
          `Node ${node.id} input ${port.id} requires at least ${minimum} active value${minimum === 1 ? "" : "s"}; found ${availableCount}.`,
          { nodeId: node.id },
        ),
      };
    }

    if (values.length > 0) {
      inputs[port.id] = isMultiValuePort(port) ? values : values[0];
      continue;
    }
    if (hasDefault) {
      inputs[port.id] = isMultiValuePort(port) ? [port.defaultValue] : port.defaultValue;
    }
  }

  return { inputs, activeEdgeCount };
}

function resolveRunOutput(
  workflow: ExecutableWorkflow,
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
): unknown {
  const nodesWithOutgoingEdges = new Set(workflow.edges.map((edge) => edge.sourceNodeId));
  const terminalNodes = workflow.nodes.filter((node) => !nodesWithOutgoingEdges.has(node.id));
  const successful = terminalNodes.flatMap((node) => {
    const result = nodeResults[node.id];
    if (result?.status !== "succeeded") {
      return [];
    }
    return [{ nodeId: node.id, outputs: result.outputs }];
  });

  if (successful.length === 0) {
    return undefined;
  }
  if (successful.length === 1) {
    const only = successful[0];
    if (!only) {
      return undefined;
    }
    return hasOwn(only.outputs, "result") ? only.outputs.result : only.outputs;
  }

  return Object.fromEntries(
    successful.map(({ nodeId, outputs }) => [
      nodeId,
      hasOwn(outputs, "result") ? outputs.result : outputs,
    ]),
  );
}

function describeAttemptFailure(
  nodeId: string,
  attempt: number,
  outcome: AttemptOutcome | undefined,
  cancelled: boolean,
  { retryable }: { retryable: boolean },
): WorkflowRunError {
  const options = {
    nodeId,
    attempt,
    ...(outcome && "cause" in outcome ? { cause: outcome.cause } : {}),
  };
  if (cancelled) {
    return fail(
      "node-cancelled",
      `Node ${nodeId} was cancelled during attempt ${attempt}.`,
      options,
    );
  }
  if (outcome?.kind === "timed-out") {
    return fail(
      "node-timed-out",
      `Node ${nodeId} attempt ${attempt}: ${outcome.cause.message}`,
      options,
    );
  }
  if (outcome?.kind === "termination-failed") {
    return fail(
      "node-termination-failed",
      `Node ${nodeId} attempt ${attempt} could not be terminated: ${toErrorMessage(outcome.cause)}`,
      options,
    );
  }
  const message = outcome && "cause" in outcome ? toErrorMessage(outcome.cause) : "unknown failure";
  if (retryable) {
    return fail(
      "retries-exhausted",
      `Node ${nodeId} failed after ${attempt} attempts: ${message}`,
      options,
    );
  }
  return fail("node-failed", `Node ${nodeId} failed: ${message}`, options);
}

/** Default simultaneous node executions per run (TECH-012); override per runner or per run. */
export const defaultWorkflowRunnerMaxWorkers = 4;

function isValidWorkerCount(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value) && value >= 1;
}

export function createWorkflowRunner(options: WorkflowRunnerOptions = {}): WorkflowRunner {
  const executors = new Map<string, WorkflowExecutorRegistration>(
    Object.entries({ ...createBuiltInExecutors(), ...(options.executors ?? {}) }).map(
      ([kind, executor]) => [kind, toRegistration(kind, executor)] as const,
    ),
  );
  const now = options.now ?? (() => new Date());
  const timers = options.timers ?? defaultWorkflowRunnerTimers;
  const defaultMaxWorkers = options.maxWorkers ?? defaultWorkflowRunnerMaxWorkers;
  if (!isValidWorkerCount(defaultMaxWorkers)) {
    throw new TypeError(
      `maxWorkers must be a positive whole number; received ${String(defaultMaxWorkers)}.`,
    );
  }

  return {
    registerExecutor(kind, executor, policy) {
      executors.set(kind, toRegistration(kind, executor, policy));
    },

    async dispatch(request) {
      const events: WorkflowRunEvent[] = [];
      const nodeResults: Record<string, WorkflowNodeResult> = {};
      const emit = (event: WorkflowRunEvent) => events.push(event);
      const timestamp = () => now().toISOString();
      const diagnostics = validateExecutableWorkflow(request.workflow as unknown);

      if (diagnostics.length > 0) {
        const error = fail(
          "invalid-workflow",
          `Compiled workflow validation failed with ${diagnostics.length} diagnostic${diagnostics.length === 1 ? "" : "s"}.`,
          { diagnostics },
        );
        emit({
          type: "run.failed",
          runId: request.runId,
          timestamp: timestamp(),
          error,
        });
        return { status: "failed", error, nodeResults, events };
      }

      const workflow = request.workflow;
      const resume = request.resume;
      const maxWorkers = request.maxWorkers ?? defaultMaxWorkers;
      if (!isValidWorkerCount(maxWorkers)) {
        const error = fail(
          "invalid-run-options",
          `maxWorkers must be a positive whole number; received ${String(maxWorkers)}.`,
        );
        emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
        return { status: "failed", error, nodeResults, events };
      }
      if (resume) {
        const problem = findCheckpointProblem(resume.checkpoint, workflow, request.runId);
        if (problem) {
          const error = fail("invalid-checkpoint", problem);
          emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
          return { status: "failed", error, nodeResults, events };
        }
        Object.assign(nodeResults, structuredClone(resume.checkpoint.completed));
        emit({
          type: "run.resumed",
          runId: request.runId,
          nodeId: resume.checkpoint.waiting.nodeId,
          taskId: resume.checkpoint.waiting.taskId,
          maxWorkers,
          timestamp: timestamp(),
        });
      } else {
        emit({ type: "run.started", runId: request.runId, maxWorkers, timestamp: timestamp() });
      }

      const nodeById = new Map(workflow.nodes.map((node) => [node.id, node] as const));
      const incomingByNodeId = new Map<string, ExecutableWorkflowEdge[]>();
      for (const edge of workflow.edges) {
        const incoming = incomingByNodeId.get(edge.targetNodeId) ?? [];
        incoming.push(edge);
        incomingByNodeId.set(edge.targetNodeId, incoming);
      }

      // Stops in-flight work when the caller cancels or another node fails.
      const runController = new AbortController();
      const onCallerAbort = () => runController.abort(request.signal?.reason);
      if (request.signal?.aborted) {
        onCallerAbort();
      }
      request.signal?.addEventListener("abort", onCallerAbort, { once: true });
      const runSignal = runController.signal;

      type NodeOutcome =
        | { kind: "succeeded" }
        | { kind: "failed"; error: WorkflowRunError }
        | { kind: "cancelled" }
        | { kind: "suspended"; checkpointWaiting: WorkflowRunCheckpoint["waiting"] };

      const executeNode = async (
        node: ExecutableWorkflowNode,
        inputs: Record<string, unknown>,
        executor: WorkflowExecutorRegistration,
      ): Promise<NodeOutcome> => {
        const nodeId = node.id;
        const resumeTask =
          resume?.checkpoint.waiting.nodeId === nodeId
            ? {
                taskId: resume.checkpoint.waiting.taskId,
                ...(resume.checkpoint.waiting.request === undefined
                  ? {}
                  : { request: resume.checkpoint.waiting.request }),
                response: resume.response,
              }
            : undefined;
        const retry = executor.policy.retry;
        const maxAttempts = retry?.maxAttempts ?? 1;
        let outcome: AttemptOutcome | undefined;
        let attempt = 0;
        while (attempt < maxAttempts) {
          if (attempt > 0 && runSignal.aborted) {
            break;
          }
          attempt += 1;
          emit({
            type: "node.started",
            runId: request.runId,
            nodeId,
            attempt,
            timestamp: timestamp(),
          });
          outcome = await runAttempt(
            executor,
            {
              runId: request.runId,
              node,
              attempt,
              inputs,
              workflowInput: request.input ?? {},
              context: request.context ?? {},
              ...(resumeTask ? { resume: resumeTask } : {}),
            },
            runSignal,
            timers,
          );
          if (
            outcome.kind === "succeeded" ||
            outcome.kind === "cancelled" ||
            outcome.kind === "termination-failed"
          ) {
            break;
          }
          const retryable = retry ? retry.isRetryable(outcome.cause, attempt) : false;
          if (!retryable || attempt >= maxAttempts) {
            break;
          }
          const delayMs = Math.max(0, retry?.backoffMs?.(attempt) ?? 0);
          emit({
            type: "node.retrying",
            runId: request.runId,
            nodeId,
            attempt,
            nextAttempt: attempt + 1,
            delayMs,
            reason: toErrorMessage(outcome.cause),
            timestamp: timestamp(),
          });
          if (!(await wait(timers, delayMs, runSignal))) {
            break;
          }
        }

        if (outcome?.kind === "succeeded" && outcome.result.suspend) {
          const suspension = outcome.result.suspend;
          emit({
            type: "node.suspended",
            runId: request.runId,
            nodeId,
            attempt,
            taskId: suspension.taskId,
            timestamp: timestamp(),
          });
          return {
            kind: "suspended",
            checkpointWaiting: {
              nodeId,
              taskId: suspension.taskId,
              ...(suspension.request === undefined ? {} : { request: suspension.request }),
              attempt,
            },
          };
        }

        if (outcome?.kind === "succeeded") {
          const outputs = outcome.result.outputs ?? {};
          nodeResults[nodeId] = { status: "succeeded", attempts: attempt, inputs, outputs };
          emit({
            type: "node.succeeded",
            runId: request.runId,
            nodeId,
            attempt,
            timestamp: timestamp(),
            outputs,
          });
          return { kind: "succeeded" };
        }

        const cancelled = outcome?.kind === "cancelled" || runSignal.aborted;
        const error = describeAttemptFailure(nodeId, attempt, outcome, cancelled, {
          retryable:
            outcome !== undefined &&
            "cause" in outcome &&
            outcome.kind !== "cancelled" &&
            retry !== undefined &&
            attempt >= maxAttempts &&
            maxAttempts > 1 &&
            retry.isRetryable(outcome.cause, attempt),
        });
        nodeResults[nodeId] = { status: "failed", attempts: attempt, inputs, outputs: {}, error };
        emit({
          type: "node.failed",
          runId: request.runId,
          nodeId,
          attempt,
          timestamp: timestamp(),
          error,
        });
        return cancelled ? { kind: "cancelled" } : { kind: "failed", error };
      };

      type Stop =
        | { kind: "failed"; error: WorkflowRunError }
        | { kind: "cancelled" }
        | { kind: "suspended"; waiting: WorkflowRunCheckpoint["waiting"] };
      let stop: Stop | undefined;
      const pending = workflow.order.filter((nodeId) => !nodeResults[nodeId]);
      const running = new Map<string, Promise<{ nodeId: string; outcome: NodeOutcome }>>();
      const isReady = (nodeId: string) =>
        (incomingByNodeId.get(nodeId) ?? []).every((edge) => nodeResults[edge.sourceNodeId]);

      // Launch ready nodes in compiled order, up to this run's worker limit (TECH-012).
      const launchReadyNodes = () => {
        let progressed = true;
        while (progressed && !stop) {
          progressed = false;
          for (const nodeId of pending.slice()) {
            if (stop || running.size >= maxWorkers) {
              return;
            }
            if (!isReady(nodeId)) {
              continue;
            }
            pending.splice(pending.indexOf(nodeId), 1);
            progressed = true;
            const node = nodeById.get(nodeId);
            if (!node) {
              throw new Error(`Validated workflow is missing node ${nodeId}.`);
            }
            const incomingEdges = incomingByNodeId.get(nodeId) ?? [];
            const built = buildInputs(node, incomingEdges, nodeResults);
            if (incomingEdges.length > 0 && built.activeEdgeCount === 0) {
              nodeResults[nodeId] = {
                status: "skipped",
                attempts: 0,
                inputs: built.inputs,
                outputs: {},
                reason: "inactive-inputs",
              };
              emit({
                type: "node.skipped",
                runId: request.runId,
                nodeId,
                timestamp: timestamp(),
                reason: "inactive-inputs",
              });
              continue;
            }
            if (built.error) {
              nodeResults[nodeId] = {
                status: "failed",
                attempts: 0,
                inputs: built.inputs,
                outputs: {},
                error: built.error,
              };
              emit({
                type: "node.failed",
                runId: request.runId,
                nodeId,
                attempt: 0,
                timestamp: timestamp(),
                error: built.error,
              });
              stop = { kind: "failed", error: built.error };
              return;
            }
            const executor = executors.get(node.kind);
            if (!executor) {
              const error = fail(
                "missing-executor",
                `No executor is registered for workflow node kind ${node.kind}.`,
                { nodeId },
              );
              nodeResults[nodeId] = {
                status: "failed",
                attempts: 0,
                inputs: built.inputs,
                outputs: {},
                error,
              };
              stop = { kind: "failed", error };
              return;
            }
            running.set(
              nodeId,
              executeNode(node, built.inputs, executor).then((outcome) => ({ nodeId, outcome })),
            );
          }
        }
      };

      try {
        while (true) {
          if (!stop && request.signal?.aborted) {
            stop = { kind: "cancelled" };
          }
          if (!stop) {
            launchReadyNodes();
          }
          if (stop && stop.kind !== "suspended" && !runSignal.aborted) {
            // A failed run starts no new work and stops what is still in flight.
            runController.abort(new Error("Workflow run stopped."));
          }
          const next = running.values().next();
          if (next.done) {
            break;
          }
          const { nodeId, outcome } = await Promise.race(running.values());
          running.delete(nodeId);
          if (stop) {
            continue;
          }
          if (outcome.kind === "failed") {
            stop = { kind: "failed", error: outcome.error };
          } else if (outcome.kind === "cancelled") {
            stop = { kind: "cancelled" };
          } else if (outcome.kind === "suspended") {
            // Let in-flight nodes finish so their results join the checkpoint.
            stop = { kind: "suspended", waiting: outcome.checkpointWaiting };
          }
        }
      } finally {
        request.signal?.removeEventListener("abort", onCallerAbort);
      }

      if (stop?.kind === "cancelled") {
        emit({ type: "run.cancelled", runId: request.runId, timestamp: timestamp() });
        return { status: "cancelled", nodeResults, events };
      }
      if (stop?.kind === "failed") {
        emit({
          type: "run.failed",
          runId: request.runId,
          timestamp: timestamp(),
          error: stop.error,
        });
        return { status: "failed", error: stop.error, nodeResults, events };
      }
      if (stop?.kind === "suspended") {
        const completed: WorkflowRunCheckpoint["completed"] = {};
        for (const [completedId, result] of Object.entries(nodeResults)) {
          if (result.status === "succeeded" || result.status === "skipped") {
            completed[completedId] = structuredClone(result);
          }
        }
        const checkpoint: WorkflowRunCheckpoint = {
          format: workflowRunCheckpointFormat,
          version: 1,
          runId: request.runId,
          workflowDigest: digestExecutableWorkflow(workflow),
          waiting: stop.waiting,
          completed,
        };
        emit({ type: "run.suspended", runId: request.runId, timestamp: timestamp(), checkpoint });
        return { status: "suspended", checkpoint, nodeResults, events };
      }

      const output = resolveRunOutput(workflow, nodeResults);
      emit({
        type: "run.succeeded",
        runId: request.runId,
        timestamp: timestamp(),
        output,
      });
      return { status: "succeeded", output, nodeResults, events };
    },
  };
}
