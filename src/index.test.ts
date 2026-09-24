import assert from "node:assert/strict";
import test from "node:test";

import {
  createWorkflowRunner,
  type ExecutableWorkflow,
  type ExecutableWorkflowPort,
} from "./index";

const anyType = { kind: "any" } as const;

function port(
  id: string,
  options: Omit<ExecutableWorkflowPort, "id" | "type"> = {},
): ExecutableWorkflowPort {
  return { id, type: anyType, ...options };
}

const branchWorkflow: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    { id: "start", kind: "control.start", outputs: [port("out")] },
    {
      id: "condition",
      kind: "json.boolean",
      outputs: [port("value")],
      data: { value: true },
    },
    {
      id: "if",
      kind: "control.if",
      inputs: [port("value"), port("condition")],
      outputs: [port("true"), port("false")],
    },
    { id: "yes", kind: "control.end", inputs: [port("in")] },
    { id: "no", kind: "control.end", inputs: [port("in")] },
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
  if (result.status !== "succeeded") {
    return;
  }
  assert.deepEqual(result.output, { value: 42 });
  assert.equal(result.nodeResults.yes?.status, "succeeded");
  assert.equal(result.nodeResults.no?.status, "skipped");
});

test("custom executor failures are not retried implicitly", async () => {
  let attempts = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.failure": () => {
        attempts += 1;
        throw new Error("permanent");
      },
    },
  });
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [{ id: "failure", kind: "custom.failure" }],
    edges: [],
    order: ["failure"],
  };

  const result = await runner.dispatch({ runId: "run-2", workflow });
  assert.equal(result.status, "failed");
  assert.equal(attempts, 1);
  if (result.status === "failed") {
    assert.equal(result.error.code, "node-failed");
  }
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
  if (result.status === "failed") {
    assert.equal(result.error.code, "missing-executor");
  }
});

test("json.array preserves compiled input-port order and default items", async () => {
  const runner = createWorkflowRunner();
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      { id: "one", kind: "json.number", outputs: [port("value")], data: { value: 1 } },
      { id: "two", kind: "json.number", outputs: [port("value")], data: { value: 2 } },
      {
        id: "array",
        kind: "json.array",
        inputs: [
          port("second"),
          port("first"),
          port("literal", { defaultValue: "tail" }),
          port("item-add", { optional: true }),
        ],
        outputs: [port("value")],
      },
      { id: "end", kind: "control.end", inputs: [port("in")] },
    ],
    edges: [
      {
        id: "e-one",
        sourceNodeId: "one",
        sourcePortId: "value",
        targetNodeId: "array",
        targetPortId: "first",
      },
      {
        id: "e-two",
        sourceNodeId: "two",
        sourcePortId: "value",
        targetNodeId: "array",
        targetPortId: "second",
      },
      {
        id: "e-end",
        sourceNodeId: "array",
        sourcePortId: "value",
        targetNodeId: "end",
        targetPortId: "in",
      },
    ],
    order: ["one", "two", "array", "end"],
  };

  const result = await runner.dispatch({ runId: "run-array", workflow });
  assert.equal(result.status, "succeeded");
  if (result.status !== "succeeded") {
    return;
  }
  assert.deepEqual(result.output, [2, 1, "tail"]);
  assert.deepEqual(
    result.nodeResults.array?.status === "succeeded" ? result.nodeResults.array.outputs : {},
    { value: [2, 1, "tail"] },
  );
});

test("aggregates many-cardinality inputs by compiled edge id", async () => {
  let observed: unknown;
  const runner = createWorkflowRunner({
    executors: {
      "custom.collect": ({ inputs }) => {
        observed = inputs.items;
        return { outputs: { result: inputs.items } };
      },
    },
  });
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      {
        id: "first",
        kind: "json.string",
        outputs: [port("value")],
        data: { value: "z-edge-value" },
      },
      {
        id: "second",
        kind: "json.string",
        outputs: [port("value")],
        data: { value: "a-edge-value" },
      },
      {
        id: "collect",
        kind: "custom.collect",
        inputs: [port("items", { cardinality: { min: 2, max: null } })],
        outputs: [port("result")],
      },
    ],
    edges: [
      {
        id: "z-edge",
        sourceNodeId: "first",
        sourcePortId: "value",
        targetNodeId: "collect",
        targetPortId: "items",
      },
      {
        id: "a-edge",
        sourceNodeId: "second",
        sourcePortId: "value",
        targetNodeId: "collect",
        targetPortId: "items",
      },
    ],
    order: ["first", "second", "collect"],
  };

  const result = await runner.dispatch({ runId: "run-many", workflow });
  assert.equal(result.status, "succeeded");
  assert.deepEqual(observed, ["a-edge-value", "z-edge-value"]);
});

test("fails readiness when active values do not satisfy runtime minimum cardinality", async () => {
  let collectCalls = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.inactive": () => ({ outputs: {} }),
      "custom.collect": () => {
        collectCalls += 1;
        return {};
      },
    },
  });
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      { id: "inactive", kind: "custom.inactive", outputs: [port("value")] },
      { id: "active", kind: "json.number", outputs: [port("value")], data: { value: 2 } },
      {
        id: "collect",
        kind: "custom.collect",
        inputs: [port("items", { cardinality: { min: 2, max: null } })],
      },
    ],
    edges: [
      {
        id: "e1",
        sourceNodeId: "inactive",
        sourcePortId: "value",
        targetNodeId: "collect",
        targetPortId: "items",
      },
      {
        id: "e2",
        sourceNodeId: "active",
        sourcePortId: "value",
        targetNodeId: "collect",
        targetPortId: "items",
      },
    ],
    order: ["inactive", "active", "collect"],
  };

  const result = await runner.dispatch({ runId: "run-not-ready", workflow });
  assert.equal(result.status, "failed");
  assert.equal(collectCalls, 0);
  if (result.status === "failed") {
    assert.equal(result.error.code, "input-not-ready");
  }
});
