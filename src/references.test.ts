import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner, validateExecutableWorkflow } from "./index.js";
import type { ExecutableWorkflow, ExecutableWorkflowDefinition } from "./types.js";

const load = (name: string) =>
  JSON.parse(
    readFileSync(new URL(`../fixtures/${name}`, import.meta.url), "utf8"),
  ) as ExecutableWorkflow;

/** Copied unchanged from workflow-editor fixtures (editor #129, #96). */
const countdown = load("compiled-v3-workflow-references.json");
const measured = load("compiled-v2-bounded-iteration-measurements.json");

const num = { kind: "typescript", source: "number" };
const port = (id: string) => ({ id, type: num });

/** walk@1: returns its own n, and recurses with n - 1 while n > 0 (dec is inactive at 0). */
function walkWorkflow(maxDepth?: number): ExecutableWorkflow {
  const ref = { workflow: "walk@1", ...(maxDepth === undefined ? {} : { maxDepth }) };
  const walk: ExecutableWorkflowDefinition = {
    id: "walk",
    version: 1,
    nodes: [
      { id: "start", kind: "control.start", outputs: [port("n")] },
      { id: "dec", kind: "math.dec", inputs: [port("n")], outputs: [port("next")] },
      {
        id: "call",
        kind: "workflow.call",
        inputs: [port("n")],
        outputs: [port("n")],
        workflowRef: ref,
      },
      { id: "end", kind: "control.end", inputs: [port("n")] },
    ],
    edges: [
      { id: "a", sourceNodeId: "start", sourcePortId: "n", targetNodeId: "dec", targetPortId: "n" },
      {
        id: "b",
        sourceNodeId: "dec",
        sourcePortId: "next",
        targetNodeId: "call",
        targetPortId: "n",
      },
      { id: "c", sourceNodeId: "start", sourcePortId: "n", targetNodeId: "end", targetPortId: "n" },
    ],
    order: ["start", "dec", "call", "end"],
    interface: {
      inputs: [{ id: "n", nodeId: "start", portId: "n", type: num }],
      outputs: [{ id: "n", nodeId: "end", portId: "n", type: num }],
    },
  };
  return {
    format: "@moritzbrantner/workflow/compiled",
    version: 3,
    nodes: [
      { id: "start", kind: "control.start", outputs: [port("n")] },
      {
        id: "call",
        kind: "workflow.call",
        inputs: [port("n")],
        outputs: [port("n")],
        workflowRef: ref,
      },
      { id: "end", kind: "control.end", inputs: [port("n")] },
    ],
    edges: [
      {
        id: "a",
        sourceNodeId: "start",
        sourcePortId: "n",
        targetNodeId: "call",
        targetPortId: "n",
      },
      { id: "b", sourceNodeId: "call", sourcePortId: "n", targetNodeId: "end", targetPortId: "n" },
    ],
    order: ["start", "call", "end"],
    workflows: { "walk@1": walk },
  };
}

function walkRunner() {
  const seen: number[] = [];
  const runner = createWorkflowRunner({
    executors: {
      "math.dec": ({ inputs }) => {
        const n = inputs.n as number;
        seen.push(n);
        return { outputs: n > 0 ? { next: n - 1 } : {} };
      },
    },
  });
  return { runner, seen };
}

test("runs a bounded recursive reference to completion through its interface", async () => {
  const { runner, seen } = walkRunner();
  const result = await runner.dispatch({
    runId: "walk",
    workflow: walkWorkflow(10),
    input: { n: 3 },
  });

  assert.equal(result.status, "succeeded");
  assert.deepEqual(result.status === "succeeded" && result.output, { n: 3 });
  assert.deepEqual(seen, [3, 2, 1, 0]);
});

test("enforces maxDepth before starting one more nested invocation", async () => {
  const { runner, seen } = walkRunner();
  const result = await runner.dispatch({
    runId: "deep",
    workflow: walkWorkflow(3),
    input: { n: 3 },
  });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "reference-depth-exhausted");
  }
  assert.deepEqual(seen, [3, 2, 1], "the fourth nesting never starts");
});

test("the editor countdown fixture is finite: it stops at maxDepth instead of looping", async () => {
  assert.deepEqual(validateExecutableWorkflow(countdown), []);
  const runner = createWorkflowRunner();
  const result = await runner.dispatch({
    runId: "countdown",
    workflow: countdown,
    input: { n: 3 },
  });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "reference-depth-exhausted");
    assert.match(result.error.message, /6 deep; maxDepth is 5/);
  }
});

test("rejects unbounded cycles, missing definitions and interface mismatches before running", () => {
  const codes = (workflow: unknown) =>
    validateExecutableWorkflow(workflow).map((diagnostic) => diagnostic.code);

  assert.ok(codes(walkWorkflow()).includes("unbounded-recursive-reference"));

  const missing = walkWorkflow(5);
  missing.nodes[1]!.workflowRef = { workflow: "other@1" };
  assert.ok(codes(missing).includes("invalid-workflow-reference"));

  const mismatch = walkWorkflow(5);
  mismatch.nodes[1]!.inputs = [port("m")];
  assert.ok(codes(mismatch).includes("invalid-workflow-reference"));

  const v1 = { ...walkWorkflow(5), version: 1 };
  assert.ok(codes(v1).includes("invalid-workflow-reference"));
});

test("reports the last completed iteration's measurements", async () => {
  const runner = createWorkflowRunner({
    executors: {
      "math.sqrt.newtonStep": ({ inputs }) => {
        const estimate = inputs.estimate as number;
        return { outputs: { next: (estimate + (inputs.target as number) / estimate) / 2 } };
      },
      "math.compare.withinTolerance": ({ inputs, node }) => ({
        outputs: {
          converged:
            Math.abs((inputs.next as number) - (inputs.previous as number)) <
            (node.data?.tolerance as number),
        },
      }),
    },
  });
  const result = await runner.dispatch({ runId: "measured", workflow: measured });

  assert.equal(result.status, "succeeded");
  const output = (result.status === "succeeded" ? result.output : {}) as {
    value: number;
    measurements: { estimate: number };
  };
  assert.equal(output.measurements.estimate, output.value);
  assert.ok(Math.abs(output.measurements.estimate - Math.SQRT2) < 1e-12);
});
