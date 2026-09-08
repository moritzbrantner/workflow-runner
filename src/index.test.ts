import assert from "node:assert/strict";
import test from "node:test";

import { createWorkflowRunner, type ExecutableWorkflow } from "./index";

const branchWorkflow: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    { id: "start", kind: "control.start", outputs: [{ id: "out" }] },
    {
      id: "condition",
      kind: "json.boolean",
      outputs: [{ id: "value" }],
      data: { value: true },
    },
    {
      id: "if",
      kind: "control.if",
      inputs: [{ id: "value" }, { id: "condition" }],
      outputs: [{ id: "true" }, { id: "false" }],
    },
    { id: "yes", kind: "control.end", inputs: [{ id: "in" }] },
    { id: "no", kind: "control.end", inputs: [{ id: "in" }] },
  ],
  edges: [
    {
      id: "e1",
      sourceNodeId: "start",
      sourcePortId: "out",
      targetNodeId: "if",
      targetPortId: "value",
    },
    {
      id: "e2",
      sourceNodeId: "condition",
      sourcePortId: "value",
      targetNodeId: "if",
      targetPortId: "condition",
    },
    {
      id: "e3",
      sourceNodeId: "if",
      sourcePortId: "true",
      targetNodeId: "yes",
      targetPortId: "in",
    },
    {
      id: "e4",
      sourceNodeId: "if",
      sourcePortId: "false",
      targetNodeId: "no",
      targetPortId: "in",
    },
  ],
  order: ["start", "condition", "if", "yes", "no"],
};

test("executes only the active branch", async () => {
  const runner = createWorkflowRunner();
  const result = await runner.dispatch({
    runId: "run-1",
    workflow: branchWorkflow,
    input: { value: 42 },
  });

  assert.equal(result.status, "succeeded");
  if (result.status !== "succeeded") return;
  assert.deepEqual(result.output, { value: 42 });
  assert.equal(result.nodeResults.yes?.status, "succeeded");
  assert.equal(result.nodeResults.no?.status, "skipped");
});

test("supports custom executors and retries", async () => {
  let attempts = 0;
  const runner = createWorkflowRunner({
    maxAttempts: 2,
    executors: {
      "custom.flaky": () => {
        attempts += 1;
        if (attempts === 1) throw new Error("temporary");
        return { outputs: { result: "ok" } };
      },
    },
  });
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [{ id: "flaky", kind: "custom.flaky" }],
    edges: [],
    order: ["flaky"],
  };

  const result = await runner.dispatch({ runId: "run-2", workflow });
  assert.equal(result.status, "succeeded");
  assert.equal(attempts, 2);
  assert.ok(result.events.some((event) => event.type === "node.retrying"));
});

test("fails when no executor is registered", async () => {
  const runner = createWorkflowRunner();
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [{ id: "missing", kind: "custom.missing" }],
    edges: [],
    order: ["missing"],
  };

  const result = await runner.dispatch({ runId: "run-3", workflow });
  assert.equal(result.status, "failed");
  if (result.status === "failed") assert.equal(result.error.code, "missing-executor");
});
