import { test } from "bun:test";
import assert from "node:assert/strict";

import { createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowRunCheckpoint } from "./types.js";

const port = (id: string) => ({ id, type: { kind: "any" } });
const edge = (id: string, source: string, sourcePort: string, target: string) => ({
  id,
  sourceNodeId: source,
  sourcePortId: sourcePort,
  targetNodeId: target,
  targetPortId: "in",
});

const approvalWorkflow: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    { id: "draft", kind: "custom.draft", outputs: [port("value")] },
    {
      id: "review",
      kind: "custom.approval",
      inputs: [port("in")],
      outputs: [port("approved"), port("rejected")],
    },
    { id: "publish", kind: "custom.publish", inputs: [port("in")], outputs: [port("result")] },
    { id: "discard", kind: "custom.discard", inputs: [port("in")], outputs: [port("result")] },
  ],
  edges: [
    edge("e1", "draft", "value", "review"),
    edge("e2", "review", "approved", "publish"),
    edge("e3", "review", "rejected", "discard"),
  ],
  order: ["draft", "review", "publish", "discard"],
};

function createApprovalRunner() {
  const calls: Record<string, number> = {};
  const count = (kind: string) => {
    calls[kind] = (calls[kind] ?? 0) + 1;
  };
  const runner = createWorkflowRunner({
    executors: {
      "custom.draft": () => {
        count("draft");
        return { outputs: { value: "release notes" } };
      },
      "custom.approval": ({ inputs, resume }) => {
        count("approval");
        if (!resume) {
          return { suspend: { taskId: "review-1", request: { approve: inputs.in } } };
        }
        const { decision } = resume.response as { decision: "approve" | "reject" };
        return {
          outputs: decision === "approve" ? { approved: inputs.in } : { rejected: inputs.in },
        };
      },
      "custom.publish": ({ inputs }) => {
        count("publish");
        return { outputs: { result: `published ${String(inputs.in)}` } };
      },
      "custom.discard": () => {
        count("discard");
        return { outputs: { result: "discarded" } };
      },
    },
  });
  return { runner, calls };
}

async function suspend(runner: ReturnType<typeof createWorkflowRunner>, runId = "run-1") {
  const result = await runner.dispatch({ runId, workflow: approvalWorkflow });
  assert.equal(result.status, "suspended");
  if (result.status !== "suspended") {
    throw new Error("expected suspension");
  }
  // The checkpoint must survive durable JSON storage unchanged.
  const stored = JSON.parse(JSON.stringify(result.checkpoint)) as WorkflowRunCheckpoint;
  assert.deepEqual(stored, result.checkpoint);
  return { result, checkpoint: stored };
}

test("suspends at a waiting node with a JSON-safe checkpoint", async () => {
  const { runner, calls } = createApprovalRunner();
  const { result, checkpoint } = await suspend(runner);

  assert.equal(checkpoint.runId, "run-1");
  assert.match(checkpoint.workflowDigest, /^sha256:[0-9a-f]{64}$/);
  assert.deepEqual(checkpoint.waiting, {
    nodeId: "review",
    taskId: "review-1",
    request: { approve: "release notes" },
    attempt: 1,
  });
  assert.deepEqual(Object.keys(checkpoint.completed), ["draft"]);
  assert.deepEqual(calls, { draft: 1, approval: 1 });
  assert.deepEqual(result.events.map((event) => event.type).slice(-2), [
    "node.suspended",
    "run.suspended",
  ]);
});

test("resumes the same run on approval without re-running completed nodes", async () => {
  const { runner, calls } = createApprovalRunner();
  const { checkpoint } = await suspend(runner);

  const resumed = await runner.dispatch({
    runId: "run-1",
    workflow: approvalWorkflow,
    resume: { checkpoint, response: { decision: "approve" } },
  });

  assert.equal(resumed.status, "succeeded");
  assert.deepEqual(calls, { draft: 1, approval: 2, publish: 1 });
  assert.equal(resumed.nodeResults.discard?.status, "skipped");
  if (resumed.status === "succeeded") {
    assert.equal(resumed.output, "published release notes");
  }
  assert.equal(resumed.events[0]?.type, "run.resumed");
});

test("routes a rejection through its declared branch", async () => {
  const { runner, calls } = createApprovalRunner();
  const { checkpoint } = await suspend(runner);

  const resumed = await runner.dispatch({
    runId: "run-1",
    workflow: approvalWorkflow,
    resume: { checkpoint, response: { decision: "reject" } },
  });

  assert.equal(resumed.status, "succeeded");
  assert.equal(resumed.nodeResults.publish?.status, "skipped");
  assert.equal(calls.discard, 1);
  assert.equal(calls.publish, undefined);
});

test("rejects a checkpoint for another run or workflow before any executor runs", async () => {
  const { runner, calls } = createApprovalRunner();
  const { checkpoint } = await suspend(runner);
  const changed: ExecutableWorkflow = {
    ...approvalWorkflow,
    nodes: approvalWorkflow.nodes.map((node) =>
      node.id === "publish" ? Object.assign({}, node, { data: { channel: "other" } }) : node,
    ),
  };

  for (const request of [
    { runId: "run-2", workflow: approvalWorkflow },
    { runId: "run-1", workflow: changed },
  ]) {
    const result = await runner.dispatch({
      ...request,
      resume: { checkpoint, response: { decision: "approve" } },
    });
    assert.equal(result.status, "failed");
    if (result.status === "failed") {
      assert.equal(result.error.code, "invalid-checkpoint");
    }
  }
  assert.deepEqual(calls, { draft: 1, approval: 1 });
});
