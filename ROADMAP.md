# Workflow Runner Roadmap

The workflow runner executes exactly one compiled workflow. It owns execution validation, node executor dispatch, data flow, retries, cancellation, and lifecycle events. It must not own workflow authoring, registration/versioning, triggers, scheduling, or queues.

## P0 — Compiled workflow conformance

Status: the MVP accepts `@moritzbrantner/workflow/compiled` version 1 and the cross-repository conformance fixture pins the current editor → runner handoff.

Next slice:

- validate runtime values rather than trusting TypeScript shapes;
- return stable diagnostics for malformed arrays/objects, duplicate ids, missing nodes or ports, invalid order, and unsupported format/version;
- reject invalid workflows before any executor is invoked;
- keep validation deterministic and side-effect free;
- preserve editor-owned port type/cardinality metadata even when the runner does not yet interpret all of it.

Related: #2.

## P0 — Define data-flow and cardinality semantics

The compiler now preserves workflow port cardinality, while the runner currently resolves inputs as one value per target port.

Next slice:

- define runtime behavior for `single`, `many`, and bounded cardinality;
- reject ambiguous fan-in instead of silently overwriting a target input;
- define deterministic ordering for multi-value inputs;
- fail execution readiness when required inputs or minimum cardinality are not satisfied;
- keep type compatibility an authoring/compiler concern unless runtime validation is explicitly required by an executor.

## P0 — Retry, timeout, and cancellation policy

- make retries opt-in by node/executor policy rather than retrying every failure indiscriminately;
- distinguish transient from permanent failures;
- add per-node execution timeouts and deterministic cancellation behavior;
- ensure cancelled/failed attempts release executor-owned resources;
- specify lifecycle events for timeout, retry exhaustion, and cancellation races.

## P1 — Deterministic parallel stages

- execute dependency-independent nodes concurrently only after the sequential semantics are fully pinned;
- derive stages from the compiled DAG deterministically;
- define stable event ordering for equal-time completions;
- preserve deterministic input aggregation and terminal output selection;
- keep a sequential mode as a reference/conformance implementation.

## P1 — Executor lifecycle and isolation

- add explicit executor setup/dispose boundaries where needed;
- support bounded concurrency and resource budgets;
- make subprocess/network adapters opt into stronger isolation and cleanup rules;
- keep ordinary runner tests hermetic and place real external integrations in broader acceptance tiers.

## P1 — Checkpoint/resume contract

- define a serializable execution checkpoint independent of engine storage;
- resume only from states that can be proven safe/idempotent;
- never re-run a successful non-idempotent executor implicitly;
- make checkpoint compatibility versioned alongside runner semantics.

## P2 — Execution evidence

- expose structured per-node timing/attempt summaries;
- provide transport-friendly lifecycle events for workflow-editor execution overlays;
- add representative DAG benchmarks for sequential and staged execution;
- keep performance claims separate from correctness/conformance gates.

## Explicit non-goals

- Workflow registration, triggers, scheduling, and run-record persistence belong to `workflow-engine`.
- Workflow authoring and compilation belong to `workflow-editor`.
- Queue ownership and distributed orchestration remain outside runner core.
