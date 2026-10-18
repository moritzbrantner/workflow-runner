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

## Buffered and streamed ports

Compiled ports may declare `payload: { mode: "buffered" | "streamed", mediaType? }` (workflow-editor Decision 1.5). A port with no payload is buffered.

- **Buffered values:** a buffered value, such as a `Uint8Array`, is passed by reference to every consumer. The producer runs once.
- **Streamed outputs:** an executor returns an `AsyncIterable` of chunks for a streamed output. Each outgoing connection receives every chunk once, in order, through a bounded per-consumer queue (`streamQueueCapacity`, default 8). The slowest active consumer backpressures the producer, so nothing is dropped and nothing is buffered without bound (TECH-004).
- **Consumers that stop early:** a consumer that finishes early, fails or is skipped releases its queue, and the others continue. The producer is closed when no consumer remains (TECH-010).
- **Run stopped:** cancelling or failing the run closes every stream.
- **Scheduling:** consumers of one stream are started together. A stream with more consumers than `maxWorkers` fails with `stream-consumers-exceed-workers` instead of deadlocking.
- **Rejected before execution:** connecting a buffered port to a streamed one fails with `payload-mode-mismatch`.
- **Missing stream:** a streamed output that is not an async iterable fails with `invalid-stream-output`.
- **JSON-safe events:** lifecycle events summarize streams and raw bytes as `{ payload: "stream" }` and `{ payload: "binary", byteLength }`, so engine run records never receive bytes or live streams.

## Selective reruns

`createWorkflowRunEvidence(request, result)` turns any finished run into JSON-safe evidence for the host or engine to store with the run (TECH-011 terminal persistence). Evidence holds:

- the workflow digest;
- a digest of the run's `{ input, context }`;
- succeeded and skipped node results whose inputs and outputs are JSON-safe. Streams and raw bytes are never recorded as reusable.

To run selected nodes again, dispatch a new run with `rerun: { evidence, nodeIds }`:

- The selected nodes and everything downstream of them execute again.
- Other recorded results are reused, so completed external work is not repeated.
- Any node without a recorded result runs normally.
- A `run.rerun` event lists the reused and re-executed nodes.
- Evidence from a different workflow version, or for a different input or context, fails with `invalid-rerun` before any executor runs. Stale results are never reused.

## Live lifecycle observer

`dispatch({ ..., onEvent })` calls the observer synchronously with each lifecycle event as it happens, for example `node.started` while an external operation is still running. The run still returns the complete `events` array, so callers without an observer are unaffected. An observer that throws never changes execution.

Event payloads summarize streams and raw bytes. Hosts that keep observed events should still bound what they retain. Workflow Engine's adapter can forward events by passing its dispatch observer through: `runner.dispatch({ ...request, onEvent: observer?.onEvent })`.

## Persisting results on completion

A compiled node with `persistence: "on-completion"` (workflow-editor TECH-011, editor PR #121) asks for its completed result to be stored immediately, not only with the terminal run result. Pass `persistNodeResult(record)` to `dispatch`. For each opted-in node, the runner sends a JSON-safe `WorkflowNodeResultRecord`:

- the run ID, workflow digest and input digest;
- the node ID and attempt;
- the iteration scope, for nodes inside an iteration body;
- the succeeded result.

The runner then behaves as follows:

- **Acknowledgment first:** the runner awaits the acknowledgment before the node counts as completed. Downstream nodes only start once the write is acknowledged, and a `node.persisted` event follows.
- **Failed or impossible write:** a rejected write, or a result that isn't JSON-safe, fails the node with `persistence-failed`, and it is never reported durable.
- **No handler:** without a handler, or for nodes without the setting, behaviour is unchanged.
- **After a lost run:** if a run is lost before terminal persistence, `createWorkflowRunEvidenceFromRecords(records)` rebuilds rerun evidence from the acknowledged top-level records. Those results are reused by a selective rerun and are not executed again.
