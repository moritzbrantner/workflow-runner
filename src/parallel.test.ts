import { test } from "bun:test";
import assert from "node:assert/strict";

import { createWorkflowRunner, defaultWorkflowRunnerMaxWorkers } from "./index.js";
import type { ExecutableWorkflow, WorkflowNodeExecutor } from "./types.js";

const port = (id: string, options: Record<string, unknown> = {}) => ({
  id,
  type: { kind: "any" },
  ...options,
});

/** `count` independent work nodes, each feeding one join node through edges `e-<n>`. */
function fanInWorkflow(count: number): ExecutableWorkflow {
  const ids = Array.from({ length: count }, (_, index) => `work-${index + 1}`);
  return {
    format: "@moritzbrantner/workflow/compiled",
    version: 1,
    nodes: [
      ...ids.map((id) => ({ id, kind: "custom.work", outputs: [port("value")] })),
      {
        id: "join",
        kind: "custom.join",
        inputs: [port("items", { cardinality: { min: count } })],
        outputs: [port("result")],
      },
    ],
    edges: ids.map((id) => ({
      id: `e-${id}`,
      sourceNodeId: id,
      sourcePortId: "value",
      targetNodeId: "join",
      targetPortId: "items",
    })),
    order: [...ids, "join"],
  };
}

/** Work nodes block until the test releases them; tracks peak simultaneous executions. */
function controlledWork() {
  let active = 0;
  let peak = 0;
  const started: string[] = [];
  const releases = new Map<string, () => void>();
  const work: WorkflowNodeExecutor = ({ node, signal }) => {
    active += 1;
    peak = Math.max(peak, active);
    started.push(node.id);
    return new Promise((resolve, reject) => {
      releases.set(node.id, () => {
        active -= 1;
        resolve({ outputs: { value: node.id } });
      });
      signal?.addEventListener("abort", () => {
        active -= 1;
        reject(new Error("aborted"));
      });
    });
  };
  return {
    work,
    started,
    peak: () => peak,
    release(nodeId: string) {
      const release = releases.get(nodeId);
      assert.ok(release, `${nodeId} has not started`);
      releases.delete(nodeId);
      release();
    },
  };
}

const tick = () => new Promise((resolve) => setImmediate(resolve));

function joinExecutor(): WorkflowNodeExecutor {
  return ({ inputs }) => ({ outputs: { result: inputs.items } });
}

test("runs independent ready nodes concurrently by default and joins in edge order", async () => {
  const control = controlledWork();
  const runner = createWorkflowRunner({
    executors: { "custom.work": control.work, "custom.join": joinExecutor() },
  });

  const pending = runner.dispatch({ runId: "parallel", workflow: fanInWorkflow(3) });
  await tick();
  assert.deepEqual(control.started, ["work-1", "work-2", "work-3"]);
  // Complete in reverse order; the join still sees compiled edge-id order.
  control.release("work-3");
  control.release("work-2");
  control.release("work-1");
  const result = await pending;

  assert.equal(control.peak(), 3);
  assert.equal(result.status, "succeeded");
  if (result.status === "succeeded") {
    assert.deepEqual(result.output, ["work-1", "work-2", "work-3"]);
  }
  const started = result.events[0];
  assert.equal(
    started?.type === "run.started" && started.maxWorkers,
    defaultWorkflowRunnerMaxWorkers,
  );
});

test("maxWorkers 1 runs strictly sequentially with the same result", async () => {
  const control = controlledWork();
  const runner = createWorkflowRunner({
    executors: { "custom.work": control.work, "custom.join": joinExecutor() },
  });

  const pending = runner.dispatch({
    runId: "sequential",
    workflow: fanInWorkflow(3),
    maxWorkers: 1,
  });
  for (const nodeId of ["work-1", "work-2", "work-3"]) {
    await tick();
    assert.equal(control.started.at(-1), nodeId);
    control.release(nodeId);
  }
  const result = await pending;

  assert.equal(control.peak(), 1);
  assert.equal(
    result.status === "succeeded" && JSON.stringify(result.output),
    '["work-1","work-2","work-3"]',
  );
});

test("never exceeds a requested worker count while every ready node progresses", async () => {
  const control = controlledWork();
  const runner = createWorkflowRunner({
    maxWorkers: 8,
    executors: { "custom.work": control.work, "custom.join": joinExecutor() },
  });

  const pending = runner.dispatch({ runId: "bounded", workflow: fanInWorkflow(5), maxWorkers: 2 });
  await tick();
  assert.deepEqual(control.started, ["work-1", "work-2"]);
  control.release("work-2");
  await tick();
  assert.deepEqual(control.started, ["work-1", "work-2", "work-3"]);
  for (const nodeId of ["work-1", "work-3", "work-4", "work-5"]) {
    await tick();
    control.release(nodeId);
  }
  const result = await pending;

  assert.equal(control.peak(), 2);
  assert.equal(result.status, "succeeded");
});

test("rejects invalid worker counts", async () => {
  assert.throws(() => createWorkflowRunner({ maxWorkers: 1.5 }), /maxWorkers/);
  const runner = createWorkflowRunner({ executors: { "custom.work": () => ({}) } });
  for (const maxWorkers of [0, -1, Number.NaN]) {
    const result = await runner.dispatch({
      runId: `invalid-${maxWorkers}`,
      workflow: fanInWorkflow(1),
      maxWorkers,
    });
    assert.equal(result.status === "failed" && result.error.code, "invalid-run-options");
  }
});

test("a failure stops in-flight work and launches nothing further", async () => {
  const control = controlledWork();
  let joinCalls = 0;
  const runner = createWorkflowRunner({
    executors: {
      "custom.work": (context) => {
        if (context.node.id === "work-2") {
          return Promise.reject(new Error("boom"));
        }
        return control.work(context);
      },
      "custom.join": () => {
        joinCalls += 1;
        return {};
      },
    },
  });

  const result = await runner.dispatch({ runId: "failing", workflow: fanInWorkflow(3) });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.nodeId, "work-2");
  }
  assert.equal(joinCalls, 0);
  const inFlight = result.nodeResults["work-1"];
  assert.equal(inFlight?.status === "failed" && inFlight.error.code, "node-cancelled");
});
