import { test } from "bun:test";
import assert from "node:assert/strict";

import { createWorkflowRunEvidence, createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowRunRequest } from "./types.js";

const port = (id: string) => ({ id, type: { kind: "any" } });
const edge = (id: string, source: string, target: string) => ({
  id,
  sourceNodeId: source,
  sourcePortId: "out",
  targetNodeId: target,
  targetPortId: "in",
});

/** extract (expensive external call) → transform → load, plus an unrelated audit branch. */
const pipeline: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    { id: "extract", kind: "external.fetch", outputs: [port("out")] },
    { id: "transform", kind: "custom.transform", inputs: [port("in")], outputs: [port("out")] },
    { id: "load", kind: "custom.load", inputs: [port("in")], outputs: [port("out")] },
    { id: "audit", kind: "custom.audit", inputs: [port("in")], outputs: [port("out")] },
  ],
  edges: [
    edge("e1", "extract", "transform"),
    edge("e2", "transform", "load"),
    edge("e3", "extract", "audit"),
  ],
  order: ["extract", "transform", "load", "audit"],
};

function pipelineRunner() {
  const calls = { extract: 0, transform: 0, load: 0, audit: 0 };
  let loadFails = true;
  const runner = createWorkflowRunner({
    executors: {
      "external.fetch": ({ workflowInput }) => {
        calls.extract += 1;
        return { outputs: { out: { rows: [1, 2, 3], source: workflowInput.source } } };
      },
      "custom.transform": ({ inputs }) => {
        calls.transform += 1;
        const { rows } = inputs.in as { rows: number[] };
        return { outputs: { out: rows.map((row) => row * 10) } };
      },
      "custom.load": ({ inputs }) => {
        calls.load += 1;
        if (loadFails) {
          throw new Error("warehouse unavailable");
        }
        return { outputs: { out: `loaded ${(inputs.in as number[]).length}` } };
      },
      "custom.audit": () => {
        calls.audit += 1;
        return { outputs: { out: "audited" } };
      },
    },
  });
  return {
    runner,
    calls,
    repair() {
      loadFails = false;
    },
  };
}

const first: WorkflowRunRequest = {
  runId: "run-1",
  workflow: pipeline,
  input: { source: "crm" },
};

test("reruns a failed node and its descendants while reusing verified upstream results", async () => {
  const { runner, calls, repair } = pipelineRunner();
  const failed = await runner.dispatch(first);
  assert.equal(failed.status, "failed");
  const evidence = createWorkflowRunEvidence(first, failed);
  // The evidence survives durable JSON storage.
  assert.deepEqual(JSON.parse(JSON.stringify(evidence)), evidence);
  assert.deepEqual(Object.keys(evidence.results).sort(), ["audit", "extract", "transform"]);

  repair();
  const rerun = await runner.dispatch({
    runId: "run-2",
    workflow: pipeline,
    input: { source: "crm" },
    rerun: { evidence, nodeIds: ["load"] },
  });

  assert.equal(rerun.status, "succeeded");
  assert.deepEqual(calls, { extract: 1, transform: 1, load: 2, audit: 1 });
  assert.equal(rerun.nodeResults.load?.status, "succeeded");
  const planned = rerun.events.find((event) => event.type === "run.rerun");
  assert.deepEqual(planned?.type === "run.rerun" && planned.rerunNodeIds, ["load"]);
});

test("rerunning an upstream node also reruns everything downstream of it", async () => {
  const { runner, calls, repair } = pipelineRunner();
  const failed = await runner.dispatch(first);
  repair();
  const rerun = await runner.dispatch({
    runId: "run-2",
    workflow: pipeline,
    input: { source: "crm" },
    rerun: { evidence: createWorkflowRunEvidence(first, failed), nodeIds: ["transform"] },
  });

  assert.equal(rerun.status, "succeeded");
  assert.deepEqual(calls, { extract: 1, transform: 2, load: 2, audit: 1 });
});

test("refuses stale evidence when the input or workflow changed", async () => {
  const { runner, calls } = pipelineRunner();
  const failed = await runner.dispatch(first);
  const evidence = createWorkflowRunEvidence(first, failed);
  const changedWorkflow: ExecutableWorkflow = {
    ...pipeline,
    nodes: pipeline.nodes.map((node) =>
      node.id === "transform" ? Object.assign({}, node, { data: { factor: 100 } }) : node,
    ),
  };

  for (const request of [
    { runId: "run-2", workflow: pipeline, input: { source: "erp" } },
    { runId: "run-3", workflow: changedWorkflow, input: { source: "crm" } },
  ]) {
    const result = await runner.dispatch({ ...request, rerun: { evidence, nodeIds: ["load"] } });
    assert.equal(result.status, "failed");
    if (result.status === "failed") {
      assert.equal(result.error.code, "invalid-rerun");
    }
  }
  assert.deepEqual(calls, { extract: 1, transform: 1, load: 1, audit: 1 });
});
