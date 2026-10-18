import { test } from "bun:test";
import assert from "node:assert/strict";

import { WorkflowAttemptTimeoutError } from "./attempts.js";
import { createWorkflowRunner } from "./index.js";
import type {
  ExecutableWorkflow,
  WorkflowExecutorPolicy,
  WorkflowNodeExecutor,
  WorkflowRunnerTimers,
} from "./types.js";

const workflow = (kind: string): ExecutableWorkflow => ({
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [{ id: "task", kind }],
  edges: [],
  order: ["task"],
});

class TransientError extends Error {}

function manualTimers() {
  const pending = new Map<number, { callback: () => void; ms: number }>();
  let next = 0;
  const timers: WorkflowRunnerTimers = {
    setTimeout(callback, ms) {
      next += 1;
      pending.set(next, { callback, ms });
      return next;
    },
    clearTimeout(handle) {
      pending.delete(handle as number);
    },
  };
  return {
    timers,
    delays: () => [...pending.values()].map((entry) => entry.ms),
    fireAll() {
      const entries = [...pending.entries()];
      pending.clear();
      for (const [, entry] of entries) {
        entry.callback();
      }
    },
  };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

const retryTransient = (maxAttempts: number): WorkflowExecutorPolicy => ({
  retry: {
    maxAttempts,
    idempotent: true,
    isRetryable: (error) => error instanceof TransientError,
  },
});

test("retries a classified transient failure until it succeeds", async () => {
  const seenAttempts: number[] = [];
  const runner = createWorkflowRunner();
  runner.registerExecutor(
    "custom.flaky",
    ({ attempt }) => {
      seenAttempts.push(attempt);
      if (attempt < 3) {
        throw new TransientError(`busy ${attempt}`);
      }
      return { outputs: { result: "done" } };
    },
    retryTransient(3),
  );

  const result = await runner.dispatch({ runId: "flaky", workflow: workflow("custom.flaky") });

  assert.equal(result.status, "succeeded");
  assert.deepEqual(seenAttempts, [1, 2, 3]);
  assert.equal(result.nodeResults.task?.attempts, 3);
  assert.deepEqual(
    result.events
      .filter((event) => event.type === "node.retrying")
      .map((event) =>
        event.type === "node.retrying"
          ? [event.nodeId, event.attempt, event.nextAttempt, event.reason]
          : [],
      ),
    [
      ["task", 1, 2, "busy 1"],
      ["task", 2, 3, "busy 2"],
    ],
  );
});

test("stops at the retry bound with a distinct exhaustion error", async () => {
  let attempts = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.down": {
        execute: () => {
          attempts += 1;
          throw new TransientError("still down");
        },
        policy: retryTransient(2),
      },
    },
  });

  const result = await runner.dispatch({ runId: "down", workflow: workflow("custom.down") });

  assert.equal(attempts, 2);
  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "retries-exhausted");
    assert.equal(result.error.attempt, 2);
  }
});

test("never retries a permanent failure, even when the policy allows retries", async () => {
  let attempts = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.permanent": {
        execute: () => {
          attempts += 1;
          throw new Error("invalid request");
        },
        policy: retryTransient(5),
      },
      "custom.default": () => {
        throw new TransientError("transient but no policy");
      },
    },
  });

  const permanent = await runner.dispatch({
    runId: "permanent",
    workflow: workflow("custom.permanent"),
  });
  const unpolicied = await runner.dispatch({
    runId: "default",
    workflow: workflow("custom.default"),
  });

  assert.equal(attempts, 1);
  assert.equal(permanent.status === "failed" && permanent.error.code, "node-failed");
  assert.equal(unpolicied.nodeResults.task?.attempts, 1);
});

test("waits the declared backoff through the timer seam", async () => {
  const clock = manualTimers();
  let attempts = 0;
  const runner = createWorkflowRunner({ timers: clock.timers });
  runner.registerExecutor(
    "custom.backoff",
    () => {
      attempts += 1;
      if (attempts === 1) {
        throw new TransientError("rate limited");
      }
      return { outputs: {} };
    },
    { retry: { ...retryTransient(2).retry!, backoffMs: (attempt) => attempt * 250 } },
  );

  const pending = runner.dispatch({ runId: "backoff", workflow: workflow("custom.backoff") });
  await tick();
  assert.deepEqual(clock.delays(), [250]);
  assert.equal(attempts, 1);
  clock.fireAll();

  assert.equal((await pending).status, "succeeded");
  assert.equal(attempts, 2);
});

test("aborts a timed-out attempt and classifies it distinctly", async () => {
  const clock = manualTimers();
  let reason: unknown;
  const waitForAbort: WorkflowNodeExecutor = ({ signal }) =>
    new Promise((_, reject) => {
      signal?.addEventListener("abort", () => {
        reason = signal.reason;
        reject(signal.reason);
      });
    });
  const runner = createWorkflowRunner({ timers: clock.timers });
  runner.registerExecutor("custom.slow", waitForAbort, { timeoutMs: 1000 });

  const pending = runner.dispatch({ runId: "slow", workflow: workflow("custom.slow") });
  await tick();
  assert.deepEqual(clock.delays(), [1000]);
  clock.fireAll();
  const result = await pending;

  assert.ok(reason instanceof WorkflowAttemptTimeoutError);
  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "node-timed-out");
  }
});

test("run cancellation aborts the active attempt and starts no retry", async () => {
  const controller = new AbortController();
  let attempts = 0;
  const runner = createWorkflowRunner();
  runner.registerExecutor(
    "custom.cancellable",
    ({ signal }) => {
      attempts += 1;
      return new Promise((_, reject) => {
        signal?.addEventListener("abort", () => reject(new TransientError("aborted")));
      });
    },
    retryTransient(3),
  );

  const pending = runner.dispatch({
    runId: "cancel",
    workflow: workflow("custom.cancellable"),
    signal: controller.signal,
  });
  await tick();
  controller.abort();
  const result = await pending;

  assert.equal(result.status, "cancelled");
  assert.equal(attempts, 1);
  const node = result.nodeResults.task;
  assert.equal(node?.status === "failed" && node.error.code, "node-cancelled");
  assert.ok(!result.events.some((event) => event.type === "node.retrying"));
});

test("terminates a non-cooperative attempt through its isolation boundary before returning", async () => {
  const clock = manualTimers();
  let finish: (() => void) | undefined;
  let terminated = false;
  let settledBeforeReturn = false;
  const runner = createWorkflowRunner({ timers: clock.timers });
  runner.registerExecutor(
    "custom.stubborn",
    () =>
      new Promise((resolve) => {
        finish = () => {
          settledBeforeReturn = true;
          resolve({ outputs: {} });
        };
      }),
    {
      timeoutMs: 50,
      terminate: () => {
        terminated = true;
        finish?.();
      },
    },
  );

  const pending = runner.dispatch({ runId: "stubborn", workflow: workflow("custom.stubborn") });
  await tick();
  clock.fireAll();
  const result = await pending;

  assert.ok(terminated);
  assert.ok(settledBeforeReturn);
  assert.equal(result.status === "failed" && result.error.code, "node-timed-out");
});

test("rejects retry policies that do not declare repeated execution safe", () => {
  const runner = createWorkflowRunner();
  assert.throws(
    () =>
      runner.registerExecutor("custom.unsafe", () => ({}), {
        retry: { maxAttempts: 2, isRetryable: () => true } as never,
      }),
    /idempotent: true/,
  );
  assert.throws(
    () => runner.registerExecutor("custom.timeout", () => ({}), { timeoutMs: 0 }),
    /timeoutMs/,
  );
});
