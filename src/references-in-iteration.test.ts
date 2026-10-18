import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner, validateExecutableWorkflow } from "./index.js";
import type { ExecutableWorkflow } from "./types.js";

/** Copied unchanged from workflow-editor fixtures/compiled-v3-reference-in-iteration.json (#141). */
const fixture = JSON.parse(
  readFileSync(
    new URL("../fixtures/compiled-v3-reference-in-iteration.json", import.meta.url),
    "utf8",
  ),
) as ExecutableWorkflow;

function variant(mutate: (workflow: any) => void): ExecutableWorkflow {
  const workflow = structuredClone(fixture);
  mutate(workflow);
  return workflow;
}

function doublingRunner() {
  const calls: number[] = [];
  const runner = createWorkflowRunner({
    executors: {
      "math.double": ({ inputs }) => {
        calls.push(inputs.in as number);
        return { outputs: { out: (inputs.in as number) * 2 } };
      },
    },
  });
  return { runner, calls };
}

test("calls the referenced workflow once per iteration", async () => {
  assert.deepEqual(validateExecutableWorkflow(fixture), []);
  const { runner, calls } = doublingRunner();
  const result = await runner.dispatch({ runId: "loop", workflow: fixture, input: { n: 1 } });

  assert.equal(result.status, "succeeded");
  assert.deepEqual(calls, [1, 2, 4]);
  assert.deepEqual(result.status === "succeeded" && result.output, {
    status: "limit-exhausted",
    value: 8,
    iterations: 3,
    maxIterations: 3,
  });
});

test("applies maxDepth and the run budget to references called from a body", async () => {
  const { runner, calls } = doublingRunner();
  const budgeted = await runner.dispatch({
    runId: "budget",
    workflow: fixture,
    input: { n: 1 },
    // Top start (1), then double + end per iteration (child start is seeded): the second
    // iteration's end is the fifth attempt and is refused.
    maxNodeAttempts: 4,
  });
  assert.equal(budgeted.status === "failed" && budgeted.error.code, "budget-exhausted");
  assert.deepEqual(calls, [1, 2]);

  // child@1 calls itself with maxDepth 2: depth 3 is refused inside the first iteration.
  const recursive = variant((workflow) => {
    const child = workflow.workflows["child@1"];
    child.nodes.push({
      id: "again",
      kind: "workflow.call",
      inputs: [{ id: "n", type: { kind: "typescript", source: "number" } }],
      outputs: [{ id: "result", type: { kind: "typescript", source: "number" } }],
      workflowRef: { workflow: "child@1", maxDepth: 2 },
    });
    child.edges.push({
      id: "start.n-again.n",
      sourceNodeId: "start",
      sourcePortId: "n",
      targetNodeId: "again",
      targetPortId: "n",
    });
    child.order.splice(1, 0, "again");
  });
  assert.deepEqual(validateExecutableWorkflow(recursive), []);
  const deep = await doublingRunner().runner.dispatch({
    runId: "deep",
    workflow: recursive,
    input: { n: 1 },
  });
  assert.equal(deep.status === "failed" && deep.error.code, "reference-depth-exhausted");
});

test("validates body references against the root closure", () => {
  const codes = (workflow: unknown) =>
    validateExecutableWorkflow(workflow).map((diagnostic) => diagnostic.code);

  const missing = variant((workflow) => {
    workflow.nodes[1].iteration.body.nodes[0].workflowRef.workflow = "missing@1";
  });
  assert.ok(codes(missing).includes("invalid-workflow-reference"));

  const mismatch = variant((workflow) => {
    workflow.nodes[1].iteration.body.nodes[0].outputs[0].id = "other";
  });
  assert.ok(codes(mismatch).includes("invalid-workflow-reference"));

  // A cycle through a body reference without any maxDepth is unbounded.
  const cycle = variant((workflow) => {
    const child = workflow.workflows["child@1"];
    child.nodes.push({
      id: "inner-loop",
      kind: "control.iterate",
      inputs: [{ id: "state", type: {} }],
      outputs: [{ id: "result", type: {} }],
      iteration: structuredClone(workflow.nodes[1].iteration),
    });
    child.order.push("inner-loop");
  });
  assert.ok(codes(cycle).includes("unbounded-recursive-reference"));

  const v2 = variant((workflow) => {
    workflow.version = 2;
    delete workflow.workflows;
  });
  assert.ok(codes(v2).includes("invalid-workflow-reference"));
});
