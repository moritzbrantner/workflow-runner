import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner, validateExecutableWorkflow } from "./index.js";
import type { ExecutableWorkflow, WorkflowNodeExecutor } from "./types.js";

/** Copied unchanged from workflow-editor fixtures/compiled-v2-bounded-iteration.json. */
const fixture = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v2-bounded-iteration.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

function variant(mutate: (iteration: Record<string, any>) => void): ExecutableWorkflow {
  const workflow = structuredClone(fixture);
  const node = workflow.nodes.find((candidate) => candidate.kind === "control.iterate")!;
  mutate(node.iteration as unknown as Record<string, any>);
  return workflow;
}

function sqrtRunner(options: { converge?: boolean } = {}) {
  const calls = { step: 0, compare: 0 };
  const step: WorkflowNodeExecutor = ({ inputs }) => {
    calls.step += 1;
    const estimate = inputs.estimate as number;
    const target = inputs.target as number;
    return { outputs: { next: (estimate + target / estimate) / 2 } };
  };
  const compare: WorkflowNodeExecutor = ({ inputs, node }) => {
    calls.compare += 1;
    const tolerance = node.data?.tolerance as number;
    const converged =
      options.converge !== false &&
      Math.abs((inputs.next as number) - (inputs.previous as number)) < tolerance;
    return { outputs: { converged } };
  };
  const runner = createWorkflowRunner({
    executors: { "math.sqrt.newtonStep": step, "math.compare.withinTolerance": compare },
  });
  return { runner, calls };
}

test("runs the editor v2 fixture until the convergence predicate succeeds", async () => {
  const { runner, calls } = sqrtRunner();
  const result = await runner.dispatch({ runId: "sqrt", workflow: fixture });

  assert.equal(result.status, "succeeded");
  if (result.status !== "succeeded") {
    return;
  }
  const output = result.output as { status: string; value: number; iterations: number };
  assert.equal(output.status, "converged");
  assert.ok(Math.abs(output.value - Math.SQRT2) < 1e-12);
  assert.ok(output.iterations > 1 && output.iterations < 10);
  assert.equal(calls.step, output.iterations);
  const iterationEvents = result.events.filter((event) => event.type === "node.iteration");
  assert.deepEqual(
    iterationEvents.map((event) => event.type === "node.iteration" && event.iteration),
    Array.from({ length: output.iterations }, (_, index) => index + 1),
  );
});

test("stops exactly at the bound with a labelled limit-exhausted candidate", async () => {
  const { runner, calls } = sqrtRunner({ converge: false });
  const result = await runner.dispatch({ runId: "no-converge", workflow: fixture });

  assert.equal(result.status, "succeeded");
  const output = result.status === "succeeded" ? (result.output as Record<string, unknown>) : {};
  assert.equal(output.status, "limit-exhausted");
  assert.equal(output.iterations, 10);
  assert.equal(output.maxIterations, 10);
  assert.equal(calls.step, 10);
  assert.ok(Math.abs((output.value as number) - Math.SQRT2) < 1e-12);
});

test("limit-only iterations never claim convergence", async () => {
  const { runner, calls } = sqrtRunner();
  const workflow = variant((iteration) => {
    iteration.maxIterations = 3;
    iteration.stop = { kind: "limit-only" };
  });
  const result = await runner.dispatch({ runId: "limit-only", workflow });

  const output = result.status === "succeeded" ? (result.output as Record<string, unknown>) : {};
  assert.equal(output.status, "limit-exhausted");
  assert.equal(calls.step, 3);
});

test("reports a missing next state as a distinct no-result failure", async () => {
  const { runner } = sqrtRunner();
  const workflow = variant((iteration) => {
    // The boundary names a declared output that the step executor never produces.
    iteration.body.nodes[0].outputs.push({ id: "unused", type: { kind: "number" } });
    iteration.body.outputs.next = { nodeId: "newton-step", portId: "unused" };
  });
  const result = await runner.dispatch({ runId: "no-result", workflow });

  assert.equal(result.status === "failed" && result.error.code, "iteration-no-result");
});

test("the run-wide attempt budget counts nested body attempts and stops before overrun", async () => {
  const { runner, calls } = sqrtRunner({ converge: false });
  // Two top-level json.number nodes, then two body attempts per iteration.
  const result = await runner.dispatch({ runId: "budget", workflow: fixture, maxNodeAttempts: 7 });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "budget-exhausted");
  }
  assert.equal(calls.step + calls.compare, 5);

  const invalid = await runner.dispatch({ runId: "bad", workflow: fixture, maxNodeAttempts: 0 });
  assert.equal(invalid.status === "failed" && invalid.error.code, "invalid-run-options");
});

test("rejects unbounded and malformed iterations, also when nested, before any executor runs", async () => {
  const cases: Array<[ExecutableWorkflow, string]> = [
    [
      variant((iteration) => {
        delete iteration.maxIterations;
      }),
      "invalid-iteration",
    ],
    [
      variant((iteration) => {
        iteration.maxIterations = Number.POSITIVE_INFINITY;
      }),
      "invalid-iteration",
    ],
    [
      variant((iteration) => {
        delete iteration.body.outputs.next;
      }),
      "invalid-iteration",
    ],
    [
      variant((iteration) => {
        const inner = structuredClone(iteration);
        inner.maxIterations = 0;
        iteration.body.nodes.push({
          id: "inner",
          kind: "control.iterate",
          inputs: [{ id: "state", type: {} }],
          outputs: [{ id: "result", type: {} }],
          iteration: inner,
        });
        iteration.body.order.push("inner");
      }),
      "invalid-iteration",
    ],
    [{ ...structuredClone(fixture), version: 1 } as ExecutableWorkflow, "invalid-iteration"],
  ];

  const { runner, calls } = sqrtRunner();
  for (const [workflow, code] of cases) {
    assert.ok(
      validateExecutableWorkflow(workflow).some((diagnostic) => diagnostic.code === code),
      code,
    );
    const result = await runner.dispatch({ runId: "invalid", workflow });
    assert.equal(result.status === "failed" && result.error.code, "invalid-workflow");
  }
  assert.deepEqual(calls, { step: 0, compare: 0 });
});

test("cancellation during an iteration is not reported as a partial result", async () => {
  const controller = new AbortController();
  const runner = createWorkflowRunner({
    executors: {
      "math.sqrt.newtonStep": ({ inputs }) => {
        controller.abort();
        return { outputs: { next: inputs.estimate } };
      },
      "math.compare.withinTolerance": () => ({ outputs: { converged: false } }),
    },
  });
  const result = await runner.dispatch({
    runId: "cancel",
    workflow: fixture,
    signal: controller.signal,
  });

  assert.equal(result.status, "cancelled");
});
