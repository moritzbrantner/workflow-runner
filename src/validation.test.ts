import assert from "node:assert/strict";
import test from "node:test";

import {
  createWorkflowRunner,
  validateExecutableWorkflow,
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

test("rejects malformed runtime values before invoking an executor", async () => {
  let calls = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.side-effect": () => {
        calls += 1;
        return {};
      },
    },
  });

  const result = await runner.dispatch({
    runId: "malformed",
    workflow: {
      format: "@moritzbrantner/workflow/compiled",
      version: 1,
      nodes: null,
      edges: [],
      order: [],
    } as unknown as ExecutableWorkflow,
  });

  assert.equal(result.status, "failed");
  assert.equal(calls, 0);
  if (result.status !== "failed") {
    return;
  }
  assert.equal(result.error.code, "invalid-workflow");
  assert.deepEqual(result.error.diagnostics?.map(({ code, path }) => ({ code, path })), [
    { code: "invalid-shape", path: "nodes" },
  ]);
  assert.deepEqual(result.events.map((event) => event.type), ["run.failed"]);
});

test("requires opaque compiled port type metadata without interpreting it", () => {
  const workflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [{ id: "node", kind: "custom.node", inputs: [{ id: "input" }] }],
    edges: [],
    order: ["node"],
  };

  assert.deepEqual(
    validateExecutableWorkflow(workflow).map(({ code, path }) => ({ code, path })),
    [{ code: "invalid-shape", path: "nodes[0].inputs[0].type" }],
  );
});

test("reports deterministic structural and graph diagnostics", () => {
  const workflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      { id: "source", kind: "custom.source", outputs: [port("out")] },
      { id: "target", kind: "custom.target", inputs: [port("in")] },
    ],
    edges: [
      {
        id: "edge-b",
        sourceNodeId: "source",
        sourcePortId: "missing",
        targetNodeId: "target",
        targetPortId: "in",
      },
      {
        id: "edge-a",
        sourceNodeId: "ghost",
        sourcePortId: "out",
        targetNodeId: "target",
        targetPortId: "missing",
      },
    ],
    order: ["target", "source"],
  };

  assert.deepEqual(
    validateExecutableWorkflow(workflow).map(({ code, path }) => ({ code, path })),
    [
      { code: "missing-node", path: "edges[1].sourceNodeId" },
      { code: "missing-port", path: "edges[0].sourcePortId" },
      { code: "missing-port", path: "edges[1].targetPortId" },
      { code: "invalid-order", path: "edges[0]" },
    ].sort((left, right) => {
      if (left.path < right.path) return -1;
      if (left.path > right.path) return 1;
      if (left.code < right.code) return -1;
      if (left.code > right.code) return 1;
      return 0;
    }),
  );
});

test("rejects ambiguous single-port fan-in before execution", async () => {
  let targetCalls = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.collect": () => {
        targetCalls += 1;
        return {};
      },
    },
  });
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      { id: "one", kind: "json.number", outputs: [port("value")], data: { value: 1 } },
      { id: "two", kind: "json.number", outputs: [port("value")], data: { value: 2 } },
      { id: "target", kind: "custom.collect", inputs: [port("in")] },
    ],
    edges: [
      {
        id: "e1",
        sourceNodeId: "one",
        sourcePortId: "value",
        targetNodeId: "target",
        targetPortId: "in",
      },
      {
        id: "e2",
        sourceNodeId: "two",
        sourcePortId: "value",
        targetNodeId: "target",
        targetPortId: "in",
      },
    ],
    order: ["one", "two", "target"],
  };

  const result = await runner.dispatch({ runId: "single-fan-in", workflow });
  assert.equal(result.status, "failed");
  assert.equal(targetCalls, 0);
  if (result.status === "failed") {
    assert.equal(result.error.code, "invalid-workflow");
    assert.equal(result.error.diagnostics?.[0]?.code, "cardinality-violation");
  }
});

test("validates declared bounded cardinality", () => {
  const workflow: ExecutableWorkflow = {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      {
        id: "node",
        kind: "custom.node",
        inputs: [port("in", { cardinality: { min: 3, max: 2 } })],
      },
    ],
    edges: [],
    order: ["node"],
  };

  assert.deepEqual(
    validateExecutableWorkflow(workflow).map(({ code, path }) => ({ code, path })),
    [{ code: "invalid-cardinality", path: "nodes[0].inputs[0].cardinality" }],
  );
});
