import type { AttemptOutcome } from "./attempts.js";
import type { WorkflowErrorCause, WorkflowRunError } from "./types.js";

export function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Executor exceptions become concise JSON-safe causes so durable hosts (Workflow Engine) record a
 * genuine workflow failure instead of an unstorable value (#25). Stacks are not included.
 */
export function toErrorCause(error: unknown): WorkflowErrorCause {
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

export function fail(
  code: WorkflowRunError["code"],
  message: string,
  options: Omit<WorkflowRunError, "code" | "message"> = {},
): WorkflowRunError {
  return { code, message, ...options };
}

export function describeAttemptFailure(
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
