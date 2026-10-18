import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner, validateExecutableWorkflow } from "./index.js";
import type { ExecutableWorkflow, ExecutableWorkflowIteration } from "./types.js";

/** Copied unchanged from workflow-editor fixtures/compiled-v2-nested-iteration.json (#97). */
const fixture = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v2-nested-iteration.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

const any = { kind: "any" };
const port = (id: string) => ({ id, type: any });

/**
 * outer: up to `outerMax` rounds; each round runs `inner` (up to `innerMax` increments of +1 that
 * converge once the value reaches a multiple of 10) and then doubles the inner result. The outer
 * loop converges when its state is at least 100.
 */
function nestedWorkflow(outerMax: number, innerMax: number): ExecutableWorkflow {
  const inner: ExecutableWorkflowIteration = {
    maxIterations: innerMax,
    state: { inputPortId: "state" },
    stop: { kind: "body-output", outputId: "converged" },
    candidate: "latest-state",
    result: { outputPortId: "result", statuses: ["converged", "limit-exhausted"] },
    body: {
      nodes: [
        {
          id: "inc",
          kind: "test.inc",
          inputs: [port("in")],
          outputs: [port("next"), port("done")],
        },
      ],
      edges: [],
      order: ["inc"],
      inputs: [{ id: "state", nodeId: "inc", portId: "in" }],
      outputs: {
        next: { nodeId: "inc", portId: "next" },
        converged: { nodeId: "inc", portId: "done" },
      },
    },
  };
  const outer: ExecutableWorkflowIteration = {
    maxIterations: outerMax,
    state: { inputPortId: "state" },
    stop: { kind: "body-output", outputId: "converged" },
    candidate: "latest-state",
    result: { outputPortId: "result", statuses: ["converged", "limit-exhausted"] },
    body: {
      nodes: [
        {
          id: "inner",
          kind: "control.iterate",
          inputs: [port("state")],
          outputs: [port("result")],
          iteration: inner,
        },
        {
          id: "double",
          kind: "test.double",
          inputs: [port("in")],
          outputs: [port("next"), port("done")],
        },
      ],
      edges: [
        {
          id: "e1",
          sourceNodeId: "inner",
          sourcePortId: "result",
          targetNodeId: "double",
          targetPortId: "in",
        },
      ],
      order: ["inner", "double"],
      inputs: [{ id: "state", nodeId: "inner", portId: "state" }],
      outputs: {
        next: { nodeId: "double", portId: "next" },
        converged: { nodeId: "double", portId: "done" },
      },
    },
  };
  return {
    format: "@moritzbrantner/workflow/compiled",
    version: 2,
    nodes: [
      { id: "seed", kind: "json.number", outputs: [port("value")], data: { value: 1 } },
      {
        id: "outer",
        kind: "control.iterate",
        inputs: [port("state")],
        outputs: [port("result")],
        iteration: outer,
      },
    ],
    edges: [
      {
        id: "e0",
        sourceNodeId: "seed",
        sourcePortId: "value",
        targetNodeId: "outer",
        targetPortId: "state",
      },
    ],
    order: ["seed", "outer"],
  };
}

function nestedRunner() {
  const calls = { inc: 0, double: 0, innerResults: [] as unknown[] };
  const runner = createWorkflowRunner({
    executors: {
      "test.inc": ({ inputs }) => {
        calls.inc += 1;
        const next = (inputs.in as number) + 1;
        return { outputs: { next, done: next % 10 === 0 } };
      },
      "test.double": ({ inputs }) => {
        calls.double += 1;
        const result = inputs.in as { value: number };
        calls.innerResults.push(result);
        const next = result.value * 2;
        return { outputs: { next, done: next >= 100 } };
      },
    },
  });
  return { runner, calls };
}

test("the editor nested-iteration fixture validates as compiled v2", async () => {
  assert.deepEqual(validateExecutableWorkflow(fixture), []);
  // Its inner loop has no inputs wired, so executing it reports the unfed input instead of guessing.
  const result = await createWorkflowRunner({
    executors: { "math.sqrt.newtonStep": () => ({}), "math.compare.withinTolerance": () => ({}) },
  }).dispatch({ runId: "fixture", workflow: fixture });
  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.match(result.error.message, /Node inner input state requires at least 1 active value/);
  }
});

test("runs the inner loop to completion for every outer iteration", async () => {
  const { runner, calls } = nestedRunner();
  const result = await runner.dispatch({ runId: "nested", workflow: nestedWorkflow(10, 20) });

  assert.equal(result.status, "succeeded");
  // 1 → inner to 10 → 20 → inner to 30 → 60 → inner to 70 → 140 (converged).
  assert.deepEqual(result.status === "succeeded" && result.output, {
    status: "converged",
    value: 140,
    iterations: 3,
    maxIterations: 10,
  });
  assert.deepEqual(
    calls.innerResults.map((inner) => (inner as { value: number; status: string }).value),
    [10, 30, 70],
  );
  assert.ok(
    calls.innerResults.every((inner) => (inner as { status: string }).status === "converged"),
  );
  assert.equal(calls.inc, 9 + 10 + 10);
});

test("each level enforces its own bound and labels its own result", async () => {
  const { runner, calls } = nestedRunner();
  const result = await runner.dispatch({ runId: "bounded", workflow: nestedWorkflow(2, 3) });

  assert.equal(result.status, "succeeded");
  assert.deepEqual(
    calls.innerResults.map((inner) => inner as { status: string; iterations: number }),
    [
      { status: "limit-exhausted", value: 4, iterations: 3, maxIterations: 3 },
      // 8 → 10 converges after two increments, inside the inner bound.
      { status: "converged", value: 10, iterations: 2, maxIterations: 3 },
    ],
  );
  assert.deepEqual(result.status === "succeeded" && result.output, {
    status: "limit-exhausted",
    value: 20,
    iterations: 2,
    maxIterations: 2,
  });
  assert.equal(calls.inc, 5);
});

test("the run-wide attempt budget counts inner attempts", async () => {
  const { runner, calls } = nestedRunner();
  // seed (1) + 5 inner increments, then the sixth increment is refused.
  const result = await runner.dispatch({
    runId: "budget",
    workflow: nestedWorkflow(10, 20),
    maxNodeAttempts: 6,
  });

  assert.equal(result.status === "failed" && result.error.code, "budget-exhausted");
  assert.equal(calls.inc, 5);
  assert.equal(calls.double, 0);
});
