import { defaultWorkflowRunnerTimers, toRegistration } from "./attempts.js";
import { createBuiltInExecutors } from "./builtins.js";
import { digestExecutableWorkflow, findCheckpointProblem } from "./checkpoint.js";
import { planRerun } from "./evidence.js";
import { resolveRunOutput } from "./node-data.js";
import type { RunContext, SharedRunState } from "./run-context.js";
import { fail } from "./run-errors.js";
import { RunScheduler } from "./run-scheduler.js";
import { RunStreams } from "./run-streams.js";
import {
  type ExecutableWorkflowEdge,
  type WorkflowExecutorRegistration,
  type WorkflowNodeResult,
  type WorkflowRunCheckpoint,
  type WorkflowRunEvent,
  type WorkflowRunRequest,
  type WorkflowRunResult,
  type WorkflowRunner,
  type WorkflowRunnerOptions,
  workflowRunCheckpointFormat,
} from "./types.js";
import { validateExecutableWorkflow } from "./validation.js";

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
    let cachedWorkflowDigest: string | undefined;
    const workflowDigest = () => (cachedWorkflowDigest ??= digestExecutableWorkflow(workflow));
    const context: RunContext = {
      request,
      workflow,
      shared,
      maxWorkers,
      nodeResults,
      timers,
      runSignal,
      emit,
      timestamp,
      workflowDigest,
      runNested: run,
    };
    const streams = new RunStreams(workflow, incomingByNodeId, streamCapacity, runSignal);
    const scheduler = new RunScheduler(context, streams, executors, incomingByNodeId, (reason) =>
      runController.abort(reason),
    );

    try {
      while (scheduler.advance()) {
        scheduler.settle(await scheduler.nextSettled());
      }
    } finally {
      request.signal?.removeEventListener("abort", onCallerAbort);
      // Close any upstream whose consumers never ran (failed, cancelled or suspended runs).
      await streams.closeAll();
    }

    const stop = scheduler.stop;
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
        workflowDigest: workflowDigest(),
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
