import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowRunCheckpoint, WorkflowRunResult } from "./types.js";

/** Copied unchanged from workflow-editor fixtures/compiled-v1-human-tasks.json (editor #105). */
const fixture = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v1-human-tasks.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

function expenseRunner() {
  const calls = { expense: 0, pay: [] as unknown[], record: [] as unknown[] };
  const runner = createWorkflowRunner({
    executors: {
      "data.value": () => {
        calls.expense += 1;
        return { outputs: { value: { id: "exp-1", amount: 100 } } };
      },
      "finance.pay": ({ inputs }) => {
        calls.pay.push(inputs.in);
        return { outputs: {} };
      },
      "finance.record": ({ inputs }) => {
        calls.record.push(inputs.in);
        return { outputs: {} };
      },
    },
  });
  return { runner, calls };
}

function checkpointOf(result: WorkflowRunResult): WorkflowRunCheckpoint {
  assert.equal(result.status, "suspended");
  if (result.status !== "suspended") {
    throw new Error("expected a suspension");
  }
  return JSON.parse(JSON.stringify(result.checkpoint)) as WorkflowRunCheckpoint;
}

test("an approval task suspends with its compiled contract and resumes on the approved branch", async () => {
  const { runner, calls } = expenseRunner();
  const checkpoint = checkpointOf(await runner.dispatch({ runId: "exp", workflow: fixture }));

  assert.equal(checkpoint.waiting.nodeId, "review");
  assert.deepEqual(checkpoint.waiting.request, {
    humanTask: {
      kind: "approval",
      prompt: "Approve this expense?",
      outcomes: ["approved", "rejected"],
    },
    request: { id: "exp-1", amount: 100 },
  });

  const resumed = await runner.dispatch({
    runId: "exp",
    workflow: fixture,
    resume: { checkpoint, response: { decision: "approved", comment: "ok" } },
  });
  assert.equal(resumed.status, "succeeded");
  assert.deepEqual(calls.pay, [{ id: "exp-1", amount: 100 }]);
  assert.deepEqual(calls.record, []);
  assert.equal(resumed.nodeResults.correct?.status, "skipped");
  assert.equal(calls.expense, 1);
});

test("a rejection takes its branch into a typed input task that continues with the value", async () => {
  const { runner, calls } = expenseRunner();
  const review = checkpointOf(await runner.dispatch({ runId: "exp", workflow: fixture }));
  const correction = checkpointOf(
    await runner.dispatch({
      runId: "exp",
      workflow: fixture,
      resume: { checkpoint: review, response: { decision: "rejected" } },
    }),
  );

  assert.equal(correction.waiting.nodeId, "correct");
  assert.deepEqual(correction.waiting.request, {
    humanTask: {
      kind: "input",
      prompt: "Enter the corrected amount.",
      responseType: { kind: "typescript", source: "number" },
    },
    context: { id: "exp-1", amount: 100 },
  });

  const done = await runner.dispatch({
    runId: "exp",
    workflow: fixture,
    resume: { checkpoint: correction, response: { value: 80 } },
  });
  assert.equal(done.status, "succeeded");
  assert.deepEqual(calls.record, [80]);
  assert.deepEqual(calls.pay, []);
  assert.equal(calls.expense, 1);
});

test("a malformed response fails the waiting node instead of choosing a branch", async () => {
  const { runner, calls } = expenseRunner();
  const checkpoint = checkpointOf(await runner.dispatch({ runId: "exp", workflow: fixture }));
  const result = await runner.dispatch({
    runId: "exp",
    workflow: fixture,
    resume: { checkpoint, response: { decision: "maybe" } },
  });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.nodeId, "review");
  }
  assert.deepEqual(calls.pay, []);
});
