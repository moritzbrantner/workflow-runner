import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  createWorkflowRunner,
  validateExecutableWorkflow,
  type ExecutableWorkflow,
} from "./index.js";

/**
 * Copied, exact compiled-v1 fixture from workflow-editor. "ComfyUI" here is
 * metadata carried to a host executor, not a model runtime supplied by runner.
 */
const compiled = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v1-external-asset.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

const outputAsset = {
  schemaVersion: 1,
  kind: "image",
  mediaType: "image/png",
  sha256: "b".repeat(64),
  byteLength: 128,
  metadata: { width: 64, height: 64 },
};

test("dispatches an opaque external asset operation exactly once with its pinned parameters", async () => {
  let dispatches = 0;
  const runner = createWorkflowRunner();
  runner.registerExecutor("asset.operation", ({ node, inputs, workflowInput }) => {
    dispatches += 1;
    assert.deepEqual(inputs.request, workflowInput);
    assert.deepEqual(node.data?.assetOperation, {
      id: "image.comfyui.generate",
      version: "1",
      parameters: {
        provider: "comfyui",
        workflow: {
          schemaVersion: 1,
          kind: "workflow",
          mediaType: "application/json",
          sha256: "a".repeat(64),
          byteLength: 256,
          metadata: { format: "comfyui-api-prompt" },
        },
      },
    });
    // Test-only output metadata, not proof that inference or an asset happened.
    return { outputs: { asset: outputAsset } };
  });

  assert.deepEqual(validateExecutableWorkflow(compiled), []);
  const before = structuredClone(compiled);
  const run = await runner.dispatch({
    runId: "external-op-run",
    workflow: compiled,
    input: { requestId: "icon-1" },
  });

  assert.equal(run.status, "succeeded");
  assert.equal(dispatches, 1);
  if (run.status !== "succeeded") {
    return;
  }
  assert.deepEqual(run.output, outputAsset);
  assert.deepEqual(run.nodeResults.generate?.outputs, { asset: outputAsset });
  assert.equal(run.nodeResults.generate?.attempts, 1);
  assert.deepEqual(
    run.events
      .filter(
        (event) =>
          event.type.startsWith("node.") && "nodeId" in event && event.nodeId === "generate",
      )
      .map((event) => event.type),
    ["node.started", "node.succeeded"],
  );
  assert.deepEqual(compiled, before);
});

test("does not infer a ComfyUI executor or retry failed external jobs", async () => {
  const unregistered = await createWorkflowRunner().dispatch({
    runId: "missing-provider",
    workflow: compiled,
  });
  assert.equal(unregistered.status, "failed");
  if (unregistered.status === "failed") {
    assert.equal(unregistered.error.code, "missing-executor");
    assert.equal(unregistered.error.nodeId, "generate");
  }

  let attempts = 0;
  const runner = createWorkflowRunner({
    executors: {
      "asset.operation": () => {
        attempts += 1;
        throw new Error("provider refused the job");
      },
    },
  });
  const failed = await runner.dispatch({ runId: "external-error", workflow: compiled });
  assert.equal(attempts, 1);
  assert.equal(failed.status, "failed");
  if (failed.status === "failed") {
    assert.equal(failed.error.code, "node-failed");
    assert.equal(failed.nodeResults.generate?.attempts, 1);
    assert.equal(failed.nodeResults.end, undefined);
  }
});
