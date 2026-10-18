import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunEvidenceFromRecords, createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowNodeResultRecord } from "./types.js";

const port = (id: string) => ({ id, type: { kind: "any" } });
const edge = (id: string, source: string, target: string) => ({
  id,
  sourceNodeId: source,
  sourcePortId: "out",
  targetNodeId: target,
  targetPortId: "in",
});

/** extract opts into on-completion persistence (editor TECH-011, PR #121); transform does not. */
const pipeline: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    { id: "extract", kind: "external.fetch", outputs: [port("out")], persistence: "on-completion" },
    { id: "transform", kind: "custom.transform", inputs: [port("in")], outputs: [port("out")] },
    { id: "load", kind: "custom.load", inputs: [port("in")], outputs: [port("out")] },
  ],
  edges: [edge("e1", "extract", "transform"), edge("e2", "transform", "load")],
  order: ["extract", "transform", "load"],
};

const tick = () => new Promise((resolve) => setImmediate(resolve));

function pipelineRunner(load: () => Promise<unknown> | unknown) {
  const calls = { extract: 0, transform: 0, load: 0 };
  const runner = createWorkflowRunner({
    executors: {
      "external.fetch": () => {
        calls.extract += 1;
        return { outputs: { out: { rows: [1, 2, 3] } } };
      },
      "custom.transform": ({ inputs }) => {
        calls.transform += 1;
        return { outputs: { out: (inputs.in as { rows: number[] }).rows.length } };
      },
      "custom.load": async () => {
        calls.load += 1;
        return { outputs: { out: await load() } };
      },
    },
  });
  return { runner, calls };
}

test("hands an opted-in result to the host before downstream work continues", async () => {
  const { runner, calls } = pipelineRunner(() => "loaded");
  const records: WorkflowNodeResultRecord[] = [];
  let acknowledge: (() => void) | undefined;

  const pending = runner.dispatch({
    runId: "run-1",
    workflow: pipeline,
    input: { source: "crm" },
    persistNodeResult: (record) => {
      records.push(record);
      return new Promise<void>((resolve) => {
        acknowledge = resolve;
      });
    },
  });
  await tick();
  assert.equal(records.length, 1);
  assert.equal(calls.transform, 0, "downstream waits for the acknowledgment");
  acknowledge?.();
  const result = await pending;

  assert.equal(result.status, "succeeded");
  assert.deepEqual(
    records.map((record) => [record.nodeId, record.attempt, record.result.outputs]),
    [["extract", 1, { out: { rows: [1, 2, 3] } }]],
  );
  assert.match(records[0]!.workflowDigest, /^sha256:/);
  assert.deepEqual(JSON.parse(JSON.stringify(records)), records);
  const persisted = result.events.filter((event) => event.type === "node.persisted");
  assert.deepEqual(
    persisted.map((event) => event.type === "node.persisted" && event.nodeId),
    ["extract"],
  );
});

test("acknowledged results survive a lost run and are reused by a rerun", async () => {
  let release: (() => void) | undefined;
  const hanging = pipelineRunner(
    () =>
      new Promise((resolve) => {
        release = () => resolve("late");
      }),
  );
  const records: WorkflowNodeResultRecord[] = [];
  const lost = hanging.runner.dispatch({
    runId: "run-1",
    workflow: pipeline,
    input: { source: "crm" },
    persistNodeResult: (record) => {
      records.push(structuredClone(record));
    },
  });
  await tick();
  await tick();
  assert.equal(hanging.calls.load, 1, "the run is still in flight when the host is lost");

  // The host restarts with only the acknowledged records.
  const evidence = createWorkflowRunEvidenceFromRecords(JSON.parse(JSON.stringify(records)));
  assert.deepEqual(Object.keys(evidence.results), ["extract"]);
  const fresh = pipelineRunner(() => "loaded");
  const rerun = await fresh.runner.dispatch({
    runId: "run-2",
    workflow: pipeline,
    input: { source: "crm" },
    rerun: { evidence, nodeIds: ["transform"] },
  });

  assert.equal(rerun.status, "succeeded");
  assert.deepEqual(fresh.calls, { extract: 0, transform: 1, load: 1 });
  release?.();
  await lost;
});

test("an unacknowledged write fails the node and is not reported durable", async () => {
  const { runner, calls } = pipelineRunner(() => "loaded");
  const result = await runner.dispatch({
    runId: "run-1",
    workflow: pipeline,
    persistNodeResult: () => Promise.reject(new Error("disk full")),
  });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "persistence-failed");
    assert.equal(result.error.nodeId, "extract");
  }
  assert.equal(calls.transform, 0);
  assert.ok(!result.events.some((event) => event.type === "node.persisted"));
});

test("runs without a persistence handler or with no opted-in nodes are unchanged", async () => {
  const { runner } = pipelineRunner(() => "loaded");
  const withoutHandler = await runner.dispatch({ runId: "run-1", workflow: pipeline });
  assert.equal(withoutHandler.status, "succeeded");
  assert.ok(!withoutHandler.events.some((event) => event.type === "node.persisted"));

  const invalid = await runner.dispatch({
    runId: "run-2",
    workflow: {
      ...pipeline,
      nodes: pipeline.nodes.map((node) =>
        node.id === "transform" ? ({ ...node, persistence: "always" } as never) : node,
      ),
    },
  });
  assert.equal(invalid.status === "failed" && invalid.error.code, "invalid-workflow");
});

test("iteration-body nodes persist each iteration's result with its scope", async () => {
  const fixture = JSON.parse(
    readFileSync(
      new URL("../fixtures/compiled-v2-bounded-iteration.json", import.meta.url),
      "utf8",
    ),
  ) as ExecutableWorkflow;
  const iterate = fixture.nodes.find((node) => node.kind === "control.iterate")!;
  iterate.iteration!.body.nodes[0]!.persistence = "on-completion";
  iterate.persistence = "on-completion";
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
  const records: WorkflowNodeResultRecord[] = [];
  const result = await runner.dispatch({
    runId: "sqrt",
    workflow: fixture,
    persistNodeResult: (record) => {
      records.push(record);
    },
  });

  assert.equal(result.status, "succeeded");
  const iterations = (result.status === "succeeded" ? result.output : {}) as { iterations: number };
  const stepRecords = records.filter((record) => record.nodeId === "newton-step");
  assert.deepEqual(
    stepRecords.map((record) => record.iteration),
    Array.from({ length: iterations.iterations }, (_, index) => ({
      nodeId: "refine",
      iteration: index + 1,
    })),
  );
  assert.equal(records.at(-1)?.nodeId, "refine");
  assert.equal(records.at(-1)?.iteration, undefined);
  assert.ok(
    stepRecords.every((record) => record.workflowDigest === records.at(-1)?.workflowDigest),
  );
});
