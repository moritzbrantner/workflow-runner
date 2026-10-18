import { test } from "bun:test";
import assert from "node:assert/strict";

import { createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow } from "./types.js";

/** Host-shaped external operation, as Workflow Engine would dispatch it. */
const workflow: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [{ id: "render", kind: "asset.operation", outputs: [{ id: "asset", type: {} }] }],
  edges: [],
  order: ["render"],
};

/** Engine stores only values that survive JSON unchanged (its JSON-safe store invariant). */
function assertJsonSafe(value: unknown) {
  assert.deepEqual(JSON.parse(JSON.stringify(value)), value);
}

test("a throwing executor yields a JSON-safe failed result and events", async () => {
  class ProviderError extends Error {
    code = "QUOTA_EXCEEDED";
    secretToken = { nested: new Map([["token", "do-not-leak"]]) };
  }
  const runner = createWorkflowRunner({
    executors: {
      "asset.operation": () => {
        throw new ProviderError("render quota exceeded");
      },
    },
  });

  const result = await runner.dispatch({ runId: "throwing", workflow });

  assert.equal(result.status, "failed");
  if (result.status !== "failed") {
    return;
  }
  assert.equal(result.error.code, "node-failed");
  assert.deepEqual(result.error.cause, {
    name: "Error",
    message: "render quota exceeded",
    code: "QUOTA_EXCEEDED",
  });
  assertJsonSafe(result.error);
  assertJsonSafe(result.events);
  assert.ok(!JSON.stringify(result).includes("do-not-leak"));
  assert.ok(!JSON.stringify(result.error).includes("at "), "no stack trace");
});

test("non-Error throws are summarized too", async () => {
  const runner = createWorkflowRunner({
    executors: {
      "asset.operation": () => {
        throw Symbol.for("odd");
      },
    },
  });
  const result = await runner.dispatch({ runId: "symbol", workflow });

  assert.equal(result.status === "failed" && result.error.cause?.message, "Symbol(odd)");
  assertJsonSafe(result.status === "failed" ? result.error : undefined);
});
