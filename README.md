# workflow-runner

Execution runtime for compiled typed DAG workflows with pluggable node executors.

## MVP

`workflow-runner` accepts the execution-neutral `@moritzbrantner/workflow/compiled` version 1 format produced by `workflow-editor` and executes one run locally in-process.

It validates the serialized compiled value before invoking any executor, then provides deterministic DAG execution, input/output propagation, inactive-branch skipping, node lifecycle events, cancellation, and a node-executor registry. Built-in executors cover `control.start`, `control.end`, `control.if`, `control.switch`, `control.merge`, and JSON primitive nodes.

The runner deliberately has no scheduler, webhook server, workflow registry, persistent queue, or distributed worker protocol. Those orchestration responsibilities belong to `workflow-engine`.

```ts
import { createWorkflowRunner } from "@moritzbrantner/workflow-runner";

const runner = createWorkflowRunner();
runner.registerExecutor("http.request", async ({ inputs }) => ({
  outputs: {
    response: await fetch(String(inputs.url)).then((response) => response.json()),
  },
}));

const result = await runner.dispatch({
  runId: "run-123",
  workflow: compiledWorkflow,
  input: { customerId: "42" },
});
```

## Compiled workflow contract

Runtime validation treats serialized input as untrusted even when TypeScript callers use `ExecutableWorkflow`. Malformed shapes, unsupported format/version values, duplicate ids or connections, missing nodes or ports, invalid DAG order, and invalid cardinality are rejected before executor dispatch. Port `type` metadata is required by compiled format v1 and is preserved as opaque compiler-owned metadata; runtime type compatibility remains an authoring/compiler concern.

Input ports without `cardinality` accept one connection. A declared `cardinality` opts a port into multi-value aggregation unless its `max` is `1`; a missing `max` is unbounded, as in the workflow-editor contract (`control.merge` declares `{ min: 1 }`). Multi-value inputs receive an array, with values ordered by compiled edge id. Declared minimum cardinality and required inputs are checked against active runtime values before the executor runs. Defaults apply only when an input has no active value. When every incoming edge is inactive, the existing branch-propagation rule skips the node.

Executor failures are not retried implicitly. Retry, timeout, and cancellation policy is an explicit runner-owned roadmap slice so hosts cannot accidentally repeat permanent or non-idempotent work.

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for the next execution/runtime slices and boundary constraints.

## Development

```sh
bun install
bun run verify
```

## Executor policies

Executors run exactly once by default; a failure is never retried implicitly (TECH-005). A registration may declare a policy:

```ts
runner.registerExecutor("model.infer", inferExecutor, {
  retry: {
    maxAttempts: 3,
    idempotent: true, // required: repeating this executor is safe or idempotently keyed
    isRetryable: (error) => error instanceof RateLimitError, // only classified transient failures
    backoffMs: (attempt) => attempt * 500,
  },
  timeoutMs: 30_000,
});
```

- The executor context carries `attempt` (1-based) for idempotency keys and provenance, and a per-attempt `signal` that aborts on run cancellation or timeout.
- A retry starts only when the failure is classified retryable and attempts remain; `node.retrying` events record the node, failed attempt, next attempt, delay and reason. Policies that allow retries without `idempotent: true` are rejected at registration.
- Failures are classified as `node-failed` (permanent), `retries-exhausted`, `node-timed-out`, `node-cancelled` (the run then ends `cancelled`, and no further attempt starts) or `node-termination-failed`.
- Interrupted attempts are never abandoned: the runner waits for the executor to settle. Executors that cannot stop cooperatively must sit behind a host-owned isolation boundary and declare `terminate` (with an optional `shutdownMs` grace period), which the runner calls when an aborted attempt keeps running (TECH-008). Terminating local work does not undo remote side effects.
- Timeouts and backoff use the injectable `timers` option, so tests need no wall-clock sleeps.

## Suspending and resuming a run

An executor can pause the run at a waiting step, such as a human approval or a typed input task, by returning `{ suspend: { taskId, request? } }`. The run then ends with `status: "suspended"` and a JSON-safe `checkpoint` (`@moritzbrantner/workflow-runner/checkpoint` version 1). The checkpoint contains:

- the run ID;
- the `sha256` digest of the canonical compiled workflow;
- the waiting node, task and attempt;
- the completed node results.

The host or engine stores the checkpoint durably (Engine TECH-001). To continue the same run, dispatch it again with `resume: { checkpoint, response }`:

- Completed nodes are restored and never executed again.
- The waiting node's executor receives `context.resume` (`taskId`, `request` and `response`) and produces its outputs, for example an `approved` or `rejected` branch.
- Execution then continues as normal.
- A checkpoint for another run ID or a different compiled workflow fails with `invalid-checkpoint` before any executor runs.

The runner defines no human-task node kinds. Those come from the compiled contract in Workflow Editor #105, implemented by host-registered executors.

## Parallel execution

Independent ready nodes run concurrently by default (TECH-012). A node starts as soon as all of its upstream nodes have finished, with no stage barrier. Ready nodes launch in compiled order.

- **Worker limit:** `createWorkflowRunner({ maxWorkers })` sets the default number of simultaneous node executions per run. The built-in default is `defaultWorkflowRunnerMaxWorkers`, which is 4.
- **Per-run override:** `dispatch({ ..., maxWorkers })` overrides the limit for that run.
- **Scope:** each run enforces its own limit; there is no pool shared across runs.
- **Sequential mode:** `1` runs nodes strictly one at a time.
- **Invalid counts:** a value that is not a positive whole number is rejected (`invalid-run-options`).
- **Evidence:** `run.started` and `run.resumed` record the effective `maxWorkers`.
- **Ordering:** fan-in values keep compiled edge-ID order whatever the completion order, and branch skipping is unchanged.
- **Failure and cancellation:** these launch no further nodes, abort in-flight attempts through their signal, and wait for them to settle. When a node suspends, other in-flight nodes finish first, and their results join the checkpoint.

## Bounded iteration (compiled version 2)

The runner executes workflow-editor's compiled-v2 `control.iterate` construct (see workflow-editor `docs/compiler.md`). Validation rejects the following before any executor runs:

- a missing, non-finite or non-positive `maxIterations`;
- malformed boundaries or body graphs;
- nested iterations;
- an iteration inside a version-1 workflow.

Each iteration runs the body as a nested graph:

- `state` and the invariant inputs are fed through the body boundaries.
- The `next` output becomes the following state.
- `node.iteration` events record each completed iteration and whether it converged.

The result output is `{ status, value, iterations, maxIterations }`:

- **`converged`:** the `converged` body output is `true`. Iteration stops immediately.
- **`limit-exhausted`:** the bound was reached. The latest state is returned as an explicitly non-converged candidate, and no further iteration is started. `limit-only` iterations always end this way.

These failures stay distinct and are never reported as partial results:

- a body that produces no next state fails with `iteration-no-result`;
- body failures fail the iteration node;
- cancellation ends the run as `cancelled`.

`maxNodeAttempts`, set on the runner or per run, is an optional run-wide budget of actual node execution attempts (TECH-006/007). It counts retries and nested body attempts and is charged before each attempt starts. When it is exhausted, the run fails with `budget-exhausted` and no further attempt starts. No default budget is imposed.
