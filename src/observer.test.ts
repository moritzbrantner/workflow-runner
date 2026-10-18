import { test } from "bun:test";
import assert from "node:assert/strict";

import { createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowRunEvent } from "./types.js";

const port = (id: string) => ({ id, type: { kind: "any" } });

/** An external asset operation, a branch with one inactive side, and a final publish step. */
const workflow: ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled",
  version: 1,
  nodes: [
    {
      id: "render",
      kind: "asset.operation",
      outputs: [port("asset")],
      data: { operation: "comfyui.render" },
    },
    {
      id: "check",
      kind: "control.if",
      inputs: [port("value"), port("condition")],
      outputs: [port("true"), port("false")],
    },
    { id: "ok", kind: "json.boolean", outputs: [port("value")], data: { value: true } },
    { id: "publish", kind: "custom.publish", inputs: [port("in")], outputs: [port("result")] },
    { id: "reject", kind: "custom.publish", inputs: [port("in")], outputs: [port("result")] },
  ],
  edges: [
    {
      id: "e1",
      sourceNodeId: "render",
      sourcePortId: "asset",
      targetNodeId: "check",
      targetPortId: "value",
    },
    {
      id: "e2",
      sourceNodeId: "ok",
      sourcePortId: "value",
      targetNodeId: "check",
      targetPortId: "condition",
    },
    {
      id: "e3",
      sourceNodeId: "check",
      sourcePortId: "true",
      targetNodeId: "publish",
      targetPortId: "in",
    },
    {
      id: "e4",
      sourceNodeId: "check",
      sourcePortId: "false",
      targetNodeId: "reject",
      targetPortId: "in",
    },
  ],
  order: ["render", "ok", "check", "publish", "reject"],
};

const tick = () => new Promise((resolve) => setImmediate(resolve));

test("a host observes lifecycle events while an external operation is still running", async () => {
  let finishRender: (() => void) | undefined;
  const runner = createWorkflowRunner({
    executors: {
      "asset.operation": () =>
        new Promise((resolve) => {
          finishRender = () => resolve({ outputs: { asset: { ref: "asset://render/1" } } });
        }),
      "custom.publish": ({ inputs }) => ({ outputs: { result: inputs.in } }),
    },
  });
  const observed: WorkflowRunEvent[] = [];

  const pending = runner.dispatch({
    runId: "live",
    workflow,
    onEvent: (event) => {
      observed.push(event);
      throw new Error("observer bug is isolated");
    },
  });
  await tick();
  assert.ok(
    observed.some((event) => event.type === "node.started" && event.nodeId === "render"),
    "render start is visible before it resolves",
  );
  assert.ok(
    !observed.some((event) => event.type === "node.succeeded" && event.nodeId === "render"),
  );

  finishRender?.();
  const result = await pending;

  assert.equal(result.status, "succeeded");
  assert.deepEqual(observed, result.events);
  assert.ok(observed.some((event) => event.type === "node.skipped" && event.nodeId === "reject"));
  assert.equal(observed.at(-1)?.type, "run.succeeded");
});

test("failure and cancellation reach the observer and match the terminal result", async () => {
  const failing = createWorkflowRunner({
    executors: {
      "asset.operation": () => {
        throw new Error("GPU unavailable");
      },
    },
  });
  const failed: WorkflowRunEvent[] = [];
  const failure = await failing.dispatch({
    runId: "fail",
    workflow,
    onEvent: (event) => failed.push(event),
  });
  assert.equal(failure.status, "failed");
  assert.deepEqual(failed, failure.events);
  assert.ok(failed.some((event) => event.type === "node.failed" && event.nodeId === "render"));

  const controller = new AbortController();
  const cancellable = createWorkflowRunner({
    executors: {
      "asset.operation": ({ signal }) =>
        new Promise((_, reject) =>
          signal?.addEventListener("abort", () => reject(new Error("aborted"))),
        ),
    },
  });
  const cancelled: WorkflowRunEvent[] = [];
  const pending = cancellable.dispatch({
    runId: "cancel",
    workflow,
    signal: controller.signal,
    onEvent: (event) => cancelled.push(event),
  });
  await tick();
  controller.abort();
  const result = await pending;
  assert.equal(result.status, "cancelled");
  assert.deepEqual(cancelled, result.events);
  assert.equal(cancelled.at(-1)?.type, "run.cancelled");
});
