import { runAttempt, wait, type AttemptOutcome } from "./attempts.js";
import { isJsonSafeValue, runInputDigest, workflowNodeResultRecordFormat } from "./evidence.js";
import { type NodeOutcome, recordNodeFailure, type RunContext } from "./run-context.js";
import { describeAttemptFailure, fail, toErrorCause, toErrorMessage } from "./run-errors.js";
import type { RunStreams } from "./run-streams.js";
import { summarizeOutputs } from "./stream.js";
import type {
  ExecutableWorkflowNode,
  WorkflowExecutorRegistration,
  WorkflowNodeResult,
  WorkflowRunError,
} from "./types.js";

/** Hands an opted-in node's result to host persistence and awaits the acknowledgment. */
export async function persistIfRequested(
  { request, shared, emit, timestamp, workflowDigest }: RunContext,
  node: ExecutableWorkflowNode,
  result: Extract<WorkflowNodeResult, { status: "succeeded" }>,
): Promise<WorkflowRunError | undefined> {
  if (node.persistence !== "on-completion" || !request.persistNodeResult) {
    return undefined;
  }
  const identity = shared.persistence ?? {
    workflowDigest: workflowDigest(),
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
}

export async function executeNode(
  context: RunContext,
  streams: RunStreams,
  node: ExecutableWorkflowNode,
  inputs: Record<string, unknown>,
  executor: WorkflowExecutorRegistration,
): Promise<NodeOutcome> {
  const { request, shared, timers, runSignal, nodeResults, emit, timestamp } = context;
  const resume = request.resume;
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
    const streamError = streams.open(node, outputs);
    if (streamError) {
      recordNodeFailure(context, nodeId, attempt, inputs, streamError);
      return { kind: "failed", error: streamError };
    }
    const succeeded = { status: "succeeded" as const, attempts: attempt, inputs, outputs };
    // Downstream nodes see the result only after an opted-in write is acknowledged.
    const persistError = await persistIfRequested(context, node, succeeded);
    if (persistError) {
      recordNodeFailure(context, nodeId, attempt, inputs, persistError);
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
    recordNodeFailure(context, nodeId, attempt, inputs, error);
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
  recordNodeFailure(context, nodeId, attempt, inputs, error);
  return cancelled ? { kind: "cancelled" } : { kind: "failed", error };
}
