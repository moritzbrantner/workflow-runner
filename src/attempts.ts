import type {
  WorkflowExecutorPolicy,
  WorkflowExecutorRegistration,
  WorkflowNodeExecutionResult,
  WorkflowNodeExecutor,
  WorkflowNodeExecutorContext,
  WorkflowRunnerTimers,
} from "./types.js";

export const defaultWorkflowRunnerTimers: WorkflowRunnerTimers = {
  setTimeout: (callback, ms) => globalThis.setTimeout(callback, ms),
  clearTimeout: (handle) => globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/** Reason passed to an attempt's AbortSignal when its timeout elapses. */
export class WorkflowAttemptTimeoutError extends Error {
  override name = "WorkflowAttemptTimeoutError" as const;
  constructor(readonly timeoutMs: number) {
    super(`Attempt timed out after ${timeoutMs} ms.`);
  }
}

const isNonNegativeFinite = (value: number) => Number.isFinite(value) && value >= 0;

/** Rejects policies that could retry unsafely or wait without bound. */
export function validateExecutorPolicy(kind: string, policy: WorkflowExecutorPolicy): void {
  const problem = (message: string) =>
    new TypeError(`Invalid execution policy for executor ${kind}: ${message}`);
  const retry = policy.retry;
  if (retry) {
    if (!Number.isInteger(retry.maxAttempts) || retry.maxAttempts < 1) {
      throw problem("retry.maxAttempts must be a whole number of at least 1.");
    }
    if (retry.idempotent !== true) {
      throw problem("retries require idempotent: true.");
    }
    if (typeof retry.isRetryable !== "function") {
      throw problem("retry.isRetryable must classify failures.");
    }
  }
  if (
    policy.timeoutMs !== undefined &&
    !(isNonNegativeFinite(policy.timeoutMs) && policy.timeoutMs > 0)
  ) {
    throw problem("timeoutMs must be a positive finite number.");
  }
  if (policy.shutdownMs !== undefined && !isNonNegativeFinite(policy.shutdownMs)) {
    throw problem("shutdownMs must be a non-negative finite number.");
  }
}

export function toRegistration(
  kind: string,
  executor: WorkflowNodeExecutor | WorkflowExecutorRegistration,
  policy: WorkflowExecutorPolicy = {},
): WorkflowExecutorRegistration {
  const registration = typeof executor === "function" ? { execute: executor, policy } : executor;
  validateExecutorPolicy(kind, registration.policy);
  return registration;
}

export type AttemptOutcome =
  | { kind: "succeeded"; result: WorkflowNodeExecutionResult }
  | { kind: "failed"; cause: unknown }
  | { kind: "timed-out"; cause: WorkflowAttemptTimeoutError }
  | { kind: "cancelled"; cause: unknown }
  | { kind: "termination-failed"; cause: unknown };

type Settled = { ok: true; result: WorkflowNodeExecutionResult } | { ok: false; cause: unknown };

/** Waits `ms` through the timer seam; resolves early (with `false`) when `signal` aborts. */
export function wait(
  timers: WorkflowRunnerTimers,
  ms: number,
  signal?: AbortSignal,
): Promise<boolean> {
  if (signal?.aborted) {
    return Promise.resolve(false);
  }
  if (ms <= 0) {
    return Promise.resolve(true);
  }
  return new Promise((resolve) => {
    const onAbort = () => {
      timers.clearTimeout(handle);
      resolve(false);
    };
    const handle = timers.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve(true);
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Runs one attempt. The attempt's signal aborts on run cancellation or timeout. An interrupted
 * attempt is never abandoned: the runner waits for it to settle, after asking a declared isolation
 * boundary to terminate it when it does not stop within `shutdownMs`.
 */
export async function runAttempt(
  registration: WorkflowExecutorRegistration,
  context: Omit<WorkflowNodeExecutorContext, "signal">,
  runSignal: AbortSignal | undefined,
  timers: WorkflowRunnerTimers,
): Promise<AttemptOutcome> {
  const { policy } = registration;
  const controller = new AbortController();
  const attemptContext: WorkflowNodeExecutorContext = { ...context, signal: controller.signal };
  let timeout: WorkflowAttemptTimeoutError | undefined;
  let timer: unknown;
  const onRunAbort = () => controller.abort(runSignal?.reason);
  if (runSignal?.aborted) {
    return { kind: "cancelled", cause: runSignal.reason };
  }
  runSignal?.addEventListener("abort", onRunAbort, { once: true });

  const interrupted = new Promise<null>((resolve) => {
    controller.signal.addEventListener("abort", () => resolve(null), { once: true });
  });
  if (policy.timeoutMs !== undefined) {
    const timeoutMs = policy.timeoutMs;
    timer = timers.setTimeout(() => {
      timeout = new WorkflowAttemptTimeoutError(timeoutMs);
      controller.abort(timeout);
    }, timeoutMs);
  }

  const settled: Promise<Settled> = Promise.resolve()
    .then(() => registration.execute(attemptContext))
    .then(
      (result) => ({ ok: true as const, result }),
      (cause: unknown) => ({ ok: false as const, cause }),
    );

  try {
    const first = await Promise.race([settled, interrupted]);
    if (first === null) {
      if (policy.terminate) {
        const stopped = await Promise.race([
          settled.then(() => true),
          wait(timers, policy.shutdownMs ?? 0).then(() => false),
        ]);
        if (!stopped) {
          try {
            await policy.terminate(attemptContext);
          } catch (cause) {
            return { kind: "termination-failed", cause };
          }
        }
      }
      await settled;
    }
    if (timeout) {
      return { kind: "timed-out", cause: timeout };
    }
    if (controller.signal.aborted) {
      return { kind: "cancelled", cause: controller.signal.reason };
    }
    const outcome = await settled;
    return outcome.ok
      ? { kind: "succeeded", result: outcome.result }
      : { kind: "failed", cause: outcome.cause };
  } finally {
    if (timer !== undefined) {
      timers.clearTimeout(timer);
    }
    runSignal?.removeEventListener("abort", onRunAbort);
  }
}
