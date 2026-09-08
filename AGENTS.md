# AGENTS.md

Apply the live coding-agent conventions for TypeScript, repository structure, dependencies, and testing.

## Boundary

`workflow-runner` executes exactly one compiled workflow run. It owns DAG execution, node lifecycle, retries, cancellation, executor dispatch, data propagation, and run events.

It does not own workflow authoring, workflow registration/versioning, cron/webhook scheduling, queues, deployment, or distributed orchestration.

The runtime contract is the serializable `@moritzbrantner/workflow/compiled` format version 1 produced by `workflow-editor`.

## Validation

Use `bun run verify` for the canonical repository check. Keep behavior tests colocated with the smallest source scope they exercise.
