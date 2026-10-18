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
  isJsonSafeValue,
  planRerun,
  runInputDigest,
  workflowNodeResultRecordFormat,
} from "./evidence.js";
import { isAsyncIterable, StreamBroadcast, summarizeOutputs } from "./stream.js";
import {
  type ExecutableWorkflow,
  type ExecutableWorkflowDefinition,
  type ExecutableWorkflowEdge,
  type ExecutableWorkflowIteration,
  type ExecutableWorkflowIterationBoundary,
  type ExecutableWorkflowNode,
  type ExecutableWorkflowPort,
  type WorkflowErrorCause,
  type WorkflowExecutorRegistration,
  type WorkflowNodeResult,
  type WorkflowRunCheckpoint,
  type WorkflowRunError,
  type WorkflowRunEvent,
  type WorkflowRunRequest,
  type WorkflowRunResult,
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

/**
 * Executor exceptions become concise JSON-safe causes so durable hosts (Workflow Engine) record a
 * genuine workflow failure instead of an unstorable value (#25). Stacks are not included.
 */
function toErrorCause(error: unknown): WorkflowErrorCause {
  if (error instanceof Error) {
    const code = (error as { code?: unknown }).code;
    return {
      name: error.name,
      message: error.message,
      ...(typeof code === "string" || (typeof code === "number" && Number.isFinite(code))
        ? { code }
        : {}),
    };
  }
  return { name: typeof error, message: String(error) };
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
  resolveValue: (edge: ExecutableWorkflowEdge, value: unknown) => unknown = (_, value) => value,
): InputBuildResult {
  const valuesByPort = new Map<string, unknown[]>();
  let activeEdgeCount = 0;

  for (const edge of [...incomingEdges].sort((left, right) => compareIds(left.id, right.id))) {
    const sourceResult = nodeResults[edge.sourceNodeId];
    if (sourceResult?.status !== "succeeded" || !hasOwn(sourceResult.outputs, edge.sourcePortId)) {
      continue;
    }
    const values = valuesByPort.get(edge.targetPortId) ?? [];
    values.push(resolveValue(edge, sourceResult.outputs[edge.sourcePortId]));
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
    ...(outcome && "cause" in outcome ? { cause: toErrorCause(outcome.cause) } : {}),
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

/** Default chunks buffered per stream consumer before the producer waits (TECH-004). */
export const defaultWorkflowRunnerStreamQueueCapacity = 8;

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
  const streamCapacity = options.streamQueueCapacity ?? defaultWorkflowRunnerStreamQueueCapacity;
  if (!isValidWorkerCount(streamCapacity)) {
    throw new TypeError(
      `streamQueueCapacity must be a positive whole number; received ${String(streamCapacity)}.`,
    );
  }
  const defaultMaxWorkers = options.maxWorkers ?? defaultWorkflowRunnerMaxWorkers;
  if (!isValidWorkerCount(defaultMaxWorkers)) {
    throw new TypeError(
      `maxWorkers must be a positive whole number; received ${String(defaultMaxWorkers)}.`,
    );
  }

  type SharedRunState = {
    /** Remaining node execution attempts for the whole run, including nested work (TECH-007). */
    budget: { remaining: number };
    /** Identity of the top-level run for node result records (TECH-011). */
    persistence?: {
      workflowDigest: string;
      inputDigest: string;
      iteration?: { nodeId: string; iteration: number };
    };
    /** Pre-completed node results injected into a nested graph (iteration boundary values). */
    seed?: Record<string, WorkflowNodeResult>;
    /** Version-3 closure of the root workflow, shared by every nested reference run. */
    workflows?: Record<string, ExecutableWorkflowDefinition>;
    /** Workflow keys of the active reference invocations, outermost first. */
    callStack?: readonly string[];
  };

  const run = async (
    request: WorkflowRunRequest,
    shared: SharedRunState,
  ): Promise<WorkflowRunResult> => {
    const events: WorkflowRunEvent[] = [];
    const nodeResults: Record<string, WorkflowNodeResult> = {};
    const emit = (event: WorkflowRunEvent) => {
      events.push(event);
      if (request.onEvent) {
        try {
          request.onEvent(event);
        } catch {
          // An observer failure never changes workflow execution (#26).
        }
      }
    };
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
    if (request.rerun && !resume) {
      const plan = planRerun(request.rerun, workflow, request);
      if ("problem" in plan) {
        const error = fail("invalid-rerun", plan.problem);
        emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
        return { status: "failed", error, nodeResults, events };
      }
      Object.assign(nodeResults, plan.reused);
      emit({
        type: "run.rerun",
        runId: request.runId,
        fromRunId: request.rerun.evidence.runId,
        reusedNodeIds: Object.keys(plan.reused),
        rerunNodeIds: plan.rerunNodeIds,
        timestamp: timestamp(),
      });
    }

    if (shared.seed) {
      Object.assign(nodeResults, shared.seed);
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

    // Streamed outputs are distributed to their consumers through bounded broadcasts (#12).
    const broadcasts = new Map<string, StreamBroadcast>();
    const streamKey = (nodeId: string, portId: string) => `${nodeId}\u0000${portId}`;
    const outgoingByPort = new Map<string, ExecutableWorkflowEdge[]>();
    for (const edge of workflow.edges) {
      const key = streamKey(edge.sourceNodeId, edge.sourcePortId);
      outgoingByPort.set(key, [...(outgoingByPort.get(key) ?? []), edge]);
    }
    const openStreams = (
      node: ExecutableWorkflowNode,
      outputs: Record<string, unknown>,
    ): WorkflowRunError | undefined => {
      for (const port of node.outputs ?? []) {
        if (port.payload?.mode !== "streamed" || !hasOwn(outputs, port.id)) {
          continue;
        }
        const value = outputs[port.id];
        if (!isAsyncIterable(value)) {
          return fail(
            "invalid-stream-output",
            `Node ${node.id} output ${port.id} is streamed but the executor did not return an async iterable.`,
            { nodeId: node.id },
          );
        }
        const edges = outgoingByPort.get(streamKey(node.id, port.id)) ?? [];
        if (edges.length > 0) {
          broadcasts.set(
            streamKey(node.id, port.id),
            new StreamBroadcast(
              value,
              edges.map((edge) => edge.id),
              streamCapacity,
            ),
          );
        }
      }
      return undefined;
    };
    const pendingReleases: Promise<void>[] = [];
    // Stopping the run (cancellation or failure) ends every stream it owns.
    runSignal.addEventListener(
      "abort",
      () => {
        for (const broadcast of broadcasts.values()) {
          pendingReleases.push(broadcast.releaseAll());
        }
      },
      { once: true },
    );
    const streamFor = (edge: ExecutableWorkflowEdge) =>
      broadcasts.get(streamKey(edge.sourceNodeId, edge.sourcePortId));
    const resolveStreamValue = (edge: ExecutableWorkflowEdge, value: unknown) =>
      streamFor(edge)?.subscribe(edge.id) ?? value;
    /** A finished, failed or skipped consumer no longer holds back its streams (TECH-010). */
    const releaseStreams = async (nodeId: string) => {
      for (const edge of incomingByNodeId.get(nodeId) ?? []) {
        await streamFor(edge)?.release(edge.id);
      }
    };

    /** Hands an opted-in node's result to host persistence and awaits the acknowledgment. */
    const persistIfRequested = async (
      node: ExecutableWorkflowNode,
      result: Extract<WorkflowNodeResult, { status: "succeeded" }>,
    ): Promise<WorkflowRunError | undefined> => {
      if (node.persistence !== "on-completion" || !request.persistNodeResult) {
        return undefined;
      }
      const identity = shared.persistence ?? {
        workflowDigest: digestExecutableWorkflow(workflow),
        inputDigest: runInputDigest(request),
      };
      if (!isJsonSafeValue(result.inputs) || !isJsonSafeValue(result.outputs)) {
        return fail(
          "persistence-failed",
          `Node ${node.id} result is not JSON-safe and cannot be persisted on completion.`,
          { nodeId: node.id, attempt: result.attempts },
        );
      }
      try {
        await request.persistNodeResult({
          format: workflowNodeResultRecordFormat,
          version: 1,
          runId: request.runId,
          workflowDigest: identity.workflowDigest,
          inputDigest: identity.inputDigest,
          nodeId: node.id,
          attempt: result.attempts,
          ...(identity.iteration ? { iteration: identity.iteration } : {}),
          result: structuredClone(result),
        });
      } catch (cause) {
        return fail(
          "persistence-failed",
          `Node ${node.id} result was not acknowledged by host persistence: ${toErrorMessage(cause)}`,
          { nodeId: node.id, attempt: result.attempts, cause: toErrorCause(cause) },
        );
      }
      emit({
        type: "node.persisted",
        runId: request.runId,
        nodeId: node.id,
        attempt: result.attempts,
        ...(identity.iteration ? { iteration: identity.iteration } : {}),
        timestamp: timestamp(),
      });
      return undefined;
    };

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
      let budgetExhausted = false;
      while (attempt < maxAttempts) {
        if (attempt > 0 && runSignal.aborted) {
          break;
        }
        if (shared.budget.remaining <= 0) {
          budgetExhausted = true;
          break;
        }
        shared.budget.remaining -= 1;
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
        const streamError = openStreams(node, outputs);
        if (streamError) {
          nodeResults[nodeId] = {
            status: "failed",
            attempts: attempt,
            inputs,
            outputs: {},
            error: streamError,
          };
          emit({
            type: "node.failed",
            runId: request.runId,
            nodeId,
            attempt,
            timestamp: timestamp(),
            error: streamError,
          });
          return { kind: "failed", error: streamError };
        }
        const succeeded = { status: "succeeded" as const, attempts: attempt, inputs, outputs };
        // Downstream nodes see the result only after an opted-in write is acknowledged.
        const persistError = await persistIfRequested(node, succeeded);
        if (persistError) {
          nodeResults[nodeId] = {
            status: "failed",
            attempts: attempt,
            inputs,
            outputs: {},
            error: persistError,
          };
          emit({
            type: "node.failed",
            runId: request.runId,
            nodeId,
            attempt,
            timestamp: timestamp(),
            error: persistError,
          });
          return { kind: "failed", error: persistError };
        }
        nodeResults[nodeId] = succeeded;
        emit({
          type: "node.succeeded",
          runId: request.runId,
          nodeId,
          attempt,
          timestamp: timestamp(),
          outputs: summarizeOutputs(outputs),
        });
        return { kind: "succeeded" };
      }

      if (budgetExhausted) {
        const error = fail(
          "budget-exhausted",
          `Run-wide node attempt budget is exhausted; node ${nodeId} attempt ${attempt + 1} was not started.`,
          { nodeId, attempt: attempt + 1 },
        );
        nodeResults[nodeId] = { status: "failed", attempts: attempt, inputs, outputs: {}, error };
        emit({
          type: "node.failed",
          runId: request.runId,
          nodeId,
          attempt,
          timestamp: timestamp(),
          error,
        });
        return { kind: "failed", error };
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

    /** Runs a compiled-v2 bounded iteration (TECH-002/006/007); the body is a nested graph run. */
    const executeIteration = async (
      node: ExecutableWorkflowNode,
      iteration: ExecutableWorkflowIteration,
      inputs: Record<string, unknown>,
    ): Promise<NodeOutcome> => {
      const nodeId = node.id;
      const finish = (error: WorkflowRunError): NodeOutcome => {
        nodeResults[nodeId] = { status: "failed", attempts: 1, inputs, outputs: {}, error };
        emit({
          type: "node.failed",
          runId: request.runId,
          nodeId,
          attempt: 1,
          timestamp: timestamp(),
          error,
        });
        return error.code === "node-cancelled" ? { kind: "cancelled" } : { kind: "failed", error };
      };
      emit({
        type: "node.started",
        runId: request.runId,
        nodeId,
        attempt: 1,
        timestamp: timestamp(),
      });
      let state = inputs.state;
      /** The last completed iteration's measurements (TECH-004); never a quality metric. */
      const measurements: Record<string, unknown> = {};
      let status: "converged" | "limit-exhausted" = "limit-exhausted";
      let iterations = 0;
      const seedNodeId = "@iteration-input";
      const body = iteration.body;
      const boundaryIds = [...new Set(body.inputs.map((input) => input.id))];
      const bodyWorkflow: ExecutableWorkflow = {
        format: workflow.format,
        // Version 2 so inner iterations in the body run as ordinary iteration nodes (#43).
        version: 2,
        nodes: [
          {
            id: seedNodeId,
            kind: seedNodeId,
            outputs: boundaryIds.map((id) => ({ id, type: { kind: "any" } })),
          },
          ...body.nodes,
        ],
        edges: [
          ...body.inputs.map((input, index) => ({
            id: `@iteration-input-${index}`,
            sourceNodeId: seedNodeId,
            sourcePortId: input.id,
            targetNodeId: input.nodeId,
            targetPortId: input.portId,
          })),
          ...body.edges,
        ],
        order: [seedNodeId, ...body.order],
      };

      while (iterations < iteration.maxIterations) {
        if (runSignal.aborted) {
          return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
        }
        const boundaryValues: Record<string, unknown> = { ...inputs, state };
        const bodyResult = await run(
          {
            runId: request.runId,
            workflow: bodyWorkflow,
            ...(request.input ? { input: request.input } : {}),
            ...(request.context ? { context: request.context } : {}),
            signal: runSignal,
            maxWorkers,
            ...(request.persistNodeResult ? { persistNodeResult: request.persistNodeResult } : {}),
          },
          {
            budget: shared.budget,
            ...(shared.workflows ? { workflows: shared.workflows } : {}),
            ...(shared.callStack ? { callStack: shared.callStack } : {}),
            persistence: {
              workflowDigest:
                shared.persistence?.workflowDigest ?? digestExecutableWorkflow(workflow),
              inputDigest: shared.persistence?.inputDigest ?? runInputDigest(request),
              iteration: { nodeId, iteration: iterations + 1 },
            },
            seed: {
              [seedNodeId]: {
                status: "succeeded",
                attempts: 0,
                inputs: {},
                outputs: Object.fromEntries(boundaryIds.map((id) => [id, boundaryValues[id]])),
              },
            },
          },
        );
        if (bodyResult.status === "cancelled") {
          return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
        }
        if (bodyResult.status === "suspended") {
          return finish(
            fail(
              "node-failed",
              `Node ${nodeId}: suspension inside an iteration body is not supported.`,
              {
                nodeId,
              },
            ),
          );
        }
        if (bodyResult.status === "failed") {
          const code =
            bodyResult.error.code === "budget-exhausted" ? "budget-exhausted" : "node-failed";
          return finish(
            fail(
              code,
              `Node ${nodeId} iteration ${iterations + 1} failed: ${bodyResult.error.message}`,
              {
                nodeId,
                attempt: 1,
                cause: bodyResult.error,
              },
            ),
          );
        }
        const read = (boundary: ExecutableWorkflowIterationBoundary | undefined) => {
          const result = boundary ? bodyResult.nodeResults[boundary.nodeId] : undefined;
          return boundary &&
            result?.status === "succeeded" &&
            hasOwn(result.outputs, boundary.portId)
            ? { value: result.outputs[boundary.portId] }
            : undefined;
        };
        const next = read(body.outputs.next);
        if (!next) {
          return finish(
            fail(
              "iteration-no-result",
              `Node ${nodeId} iteration ${iterations + 1} produced no next state.`,
              { nodeId },
            ),
          );
        }
        iterations += 1;
        state = next.value;
        for (const measurement of body.outputs.measurements ?? []) {
          const measured = read(measurement);
          if (measured) {
            measurements[measurement.id] = measured.value;
          } else {
            delete measurements[measurement.id];
          }
        }
        const converged =
          iteration.stop.kind === "body-output" && read(body.outputs.converged)?.value === true;
        emit({
          type: "node.iteration",
          runId: request.runId,
          nodeId,
          iteration: iterations,
          converged,
          timestamp: timestamp(),
        });
        if (converged) {
          status = "converged";
          break;
        }
      }

      const outputs = {
        [iteration.result.outputPortId]: {
          status,
          value: state,
          iterations,
          maxIterations: iteration.maxIterations,
          ...(iteration.body.outputs.measurements ? { measurements } : {}),
        },
      };
      const succeeded = { status: "succeeded" as const, attempts: 1, inputs, outputs };
      const persistError = await persistIfRequested(node, succeeded);
      if (persistError) {
        return finish(persistError);
      }
      nodeResults[nodeId] = succeeded;
      emit({
        type: "node.succeeded",
        runId: request.runId,
        nodeId,
        attempt: 1,
        timestamp: timestamp(),
        outputs,
      });
      return { kind: "succeeded" };
    };

    /**
     * Runs a version-3 Workflow Reference (editor TECH-005): the node's inputs feed the referenced
     * definition's interface inputs and its interface outputs become the node's outputs. maxDepth
     * bounds how many invocations of that definition may be nested (the outermost counts as 1).
     */
    const executeReference = async (
      node: ExecutableWorkflowNode,
      reference: NonNullable<ExecutableWorkflowNode["workflowRef"]>,
      inputs: Record<string, unknown>,
    ): Promise<NodeOutcome> => {
      const nodeId = node.id;
      const finish = (error: WorkflowRunError): NodeOutcome => {
        nodeResults[nodeId] = { status: "failed", attempts: 1, inputs, outputs: {}, error };
        emit({
          type: "node.failed",
          runId: request.runId,
          nodeId,
          attempt: 1,
          timestamp: timestamp(),
          error,
        });
        return error.code === "node-cancelled" ? { kind: "cancelled" } : { kind: "failed", error };
      };
      const workflows = shared.workflows ?? workflow.workflows ?? {};
      const definition = workflows[reference.workflow];
      const callStack = shared.callStack ?? [];
      const depth = callStack.filter((key) => key === reference.workflow).length + 1;
      if (!definition) {
        return finish(
          fail("node-failed", `Workflow ${reference.workflow} is not in the compiled closure.`, {
            nodeId,
          }),
        );
      }
      if (reference.maxDepth !== undefined && depth > reference.maxDepth) {
        return finish(
          fail(
            "reference-depth-exhausted",
            `Node ${nodeId} would nest ${reference.workflow} ${depth} deep; maxDepth is ${reference.maxDepth}.`,
            { nodeId },
          ),
        );
      }
      emit({
        type: "node.started",
        runId: request.runId,
        nodeId,
        attempt: 1,
        timestamp: timestamp(),
      });
      const seed: Record<string, WorkflowNodeResult> = {};
      for (const port of definition.interface.inputs) {
        if (!hasOwn(inputs, port.id)) {
          continue;
        }
        const entry = seed[port.nodeId] ?? {
          status: "succeeded" as const,
          attempts: 0,
          inputs: {},
          outputs: {},
        };
        (entry.outputs as Record<string, unknown>)[port.portId] = inputs[port.id];
        seed[port.nodeId] = entry;
      }
      const result = await run(
        {
          runId: request.runId,
          workflow: {
            format: workflow.format,
            version: 3,
            nodes: definition.nodes,
            edges: definition.edges,
            order: definition.order,
            workflows,
          },
          ...(request.input ? { input: request.input } : {}),
          ...(request.context ? { context: request.context } : {}),
          signal: runSignal,
          maxWorkers,
          ...(request.persistNodeResult ? { persistNodeResult: request.persistNodeResult } : {}),
        },
        {
          budget: shared.budget,
          workflows,
          callStack: [...callStack, reference.workflow],
          seed,
          persistence: shared.persistence ?? {
            workflowDigest: digestExecutableWorkflow(workflow),
            inputDigest: runInputDigest(request),
          },
        },
      );
      if (result.status === "cancelled") {
        return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
      }
      if (result.status === "suspended") {
        return finish(
          fail(
            "node-failed",
            `Node ${nodeId}: suspension inside a referenced workflow is not supported.`,
            {
              nodeId,
            },
          ),
        );
      }
      if (result.status === "failed") {
        const passThrough = ["reference-depth-exhausted", "budget-exhausted"] as const;
        const code = (passThrough as readonly string[]).includes(result.error.code)
          ? result.error.code
          : "node-failed";
        return finish(
          fail(code, `Node ${nodeId} (${reference.workflow}) failed: ${result.error.message}`, {
            nodeId,
            attempt: 1,
            cause: result.error,
          }),
        );
      }
      const outputs: Record<string, unknown> = {};
      for (const port of definition.interface.outputs) {
        const end = result.nodeResults[port.nodeId];
        if (end?.status === "succeeded" && hasOwn(end.inputs, port.portId)) {
          outputs[port.id] = end.inputs[port.portId];
        }
      }
      const succeeded = { status: "succeeded" as const, attempts: 1, inputs, outputs };
      const persistError = await persistIfRequested(node, succeeded);
      if (persistError) {
        return finish(persistError);
      }
      nodeResults[nodeId] = succeeded;
      emit({
        type: "node.succeeded",
        runId: request.runId,
        nodeId,
        attempt: 1,
        timestamp: timestamp(),
        outputs: summarizeOutputs(outputs),
      });
      return { kind: "succeeded" };
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
    /** Prepares and starts one ready node; returns false when the run must stop. */
    const launchNode = (nodeId: string): boolean => {
      pending.splice(pending.indexOf(nodeId), 1);
      const node = nodeById.get(nodeId);
      if (!node) {
        throw new Error(`Validated workflow is missing node ${nodeId}.`);
      }
      const incomingEdges = incomingByNodeId.get(nodeId) ?? [];
      const built = buildInputs(node, incomingEdges, nodeResults, resolveStreamValue);
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
        pendingReleases.push(releaseStreams(nodeId));
        return true;
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
        return false;
      }
      const finish = (outcome: NodeOutcome) =>
        releaseStreams(nodeId).then(() => ({ nodeId, outcome }));
      if (node.kind === "control.iterate" && node.iteration) {
        running.set(nodeId, executeIteration(node, node.iteration, built.inputs).then(finish));
        return true;
      }
      if (node.workflowRef) {
        running.set(nodeId, executeReference(node, node.workflowRef, built.inputs).then(finish));
        return true;
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
        return false;
      }
      running.set(nodeId, executeNode(node, built.inputs, executor).then(finish));
      return true;
    };

    /**
     * Consumers of one stream start together so a consumer waiting for a worker slot never
     * backpressures the producer into a deadlock.
     */
    const streamGroup = (nodeId: string): string[] => {
      const group = new Set<string>([nodeId]);
      for (const edge of incomingByNodeId.get(nodeId) ?? []) {
        if (!streamFor(edge)) {
          continue;
        }
        for (const sibling of outgoingByPort.get(streamKey(edge.sourceNodeId, edge.sourcePortId)) ??
          []) {
          if (pending.includes(sibling.targetNodeId)) {
            group.add(sibling.targetNodeId);
          }
        }
      }
      return workflow.order.filter((id) => group.has(id));
    };

    // Launch ready nodes in compiled order, up to this run's worker limit (TECH-012).
    const launchReadyNodes = () => {
      let progressed = true;
      while (progressed && !stop) {
        progressed = false;
        for (const nodeId of pending.slice()) {
          if (stop || running.size >= maxWorkers) {
            return;
          }
          if (!pending.includes(nodeId) || !isReady(nodeId)) {
            continue;
          }
          const group = streamGroup(nodeId);
          if (group.length > maxWorkers) {
            stop = {
              kind: "failed",
              error: fail(
                "stream-consumers-exceed-workers",
                `Nodes ${group.join(", ")} consume one stream together and need ${group.length} workers; maxWorkers is ${maxWorkers}.`,
                { nodeId },
              ),
            };
            return;
          }
          if (!group.every(isReady) || running.size + group.length > maxWorkers) {
            continue;
          }
          progressed = true;
          for (const member of group) {
            if (!launchNode(member)) {
              return;
            }
          }
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
          if (!stop && running.size === 0 && pending.length > 0) {
            stop = {
              kind: "failed",
              error: fail(
                "stream-deadlock",
                `Nodes ${pending.join(", ")} can never start: their stream consumers cannot run together.`,
              ),
            };
          }
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
      // Close any upstream whose consumers never ran (failed, cancelled or suspended runs).
      for (const broadcast of broadcasts.values()) {
        pendingReleases.push(broadcast.releaseAll());
      }
      await Promise.allSettled(pendingReleases);
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
  };

  return {
    registerExecutor(kind, executor, policy) {
      executors.set(kind, toRegistration(kind, executor, policy));
    },

    dispatch(request) {
      const budget = request.maxNodeAttempts ?? options.maxNodeAttempts;
      if (budget !== undefined && !isValidWorkerCount(budget)) {
        const error = fail(
          "invalid-run-options",
          `maxNodeAttempts must be a positive whole number; received ${String(budget)}.`,
        );
        return Promise.resolve({
          status: "failed",
          error,
          nodeResults: {},
          events: [
            { type: "run.failed", runId: request.runId, timestamp: now().toISOString(), error },
          ],
        });
      }
      return run(request, {
        budget: { remaining: budget ?? Number.POSITIVE_INFINITY },
        ...(request.workflow.workflows ? { workflows: request.workflow.workflows } : {}),
        callStack: [],
      });
    },
  };
}
