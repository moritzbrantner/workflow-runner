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
