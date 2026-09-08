import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { createWorkflowRunner, type ExecutableWorkflow } from "./index";

const compiledV1Fixture = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v1-simple.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

test("executes the canonical editor compiled-v1 fixture", async () => {
  const runner = createWorkflowRunner();
  const result = await runner.dispatch({
    runId: "compiled-v1-conformance",
    workflow: compiledV1Fixture,
    input: { message: "hello" },
  });

  assert.equal(result.status, "succeeded");
  if (result.status !== "succeeded") return;

  assert.deepEqual(result.output, { message: "hello" });
  assert.equal(result.nodeResults.start?.status, "succeeded");
  assert.equal(result.nodeResults.end?.status, "succeeded");
});
