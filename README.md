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

Ordinary input ports accept one connection. Explicit `cardinality.max: null` or `max > 1` opts a port into multi-value aggregation, with values ordered by compiled edge id. Declared minimum cardinality and required inputs are checked against active runtime values before the executor runs. Defaults apply only when an input has no active value. When every incoming edge is inactive, the existing branch-propagation rule skips the node.

Executor failures are not retried implicitly. Retry, timeout, and cancellation policy is an explicit runner-owned roadmap slice so hosts cannot accidentally repeat permanent or non-idempotent work.

## Roadmap

See [ROADMAP.md](./ROADMAP.md) for the next execution/runtime slices and boundary constraints.

## Development

```sh
bun install
bun run verify
```
