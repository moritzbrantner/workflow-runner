# Workflow Runner Roadmap

The workflow runner executes exactly one compiled workflow. It owns execution validation, node executor dispatch, data flow, retries, cancellation, and lifecycle events. It must not own workflow authoring, registration/versioning, triggers, scheduling, or queues.

## P0 — Compiled workflow conformance

Status: implemented.

- validate serialized runtime values rather than trusting TypeScript shapes;
- return stable diagnostics for malformed arrays/objects, duplicate ids/connections, missing nodes or ports, invalid order/cardinality, and unsupported format/version;
- reject invalid workflows before any executor is invoked;
- keep validation deterministic and side-effect free;
- require and preserve editor-owned port type/cardinality metadata without re-owning type compatibility.

Conformance remains pinned by the editor-produced compiled-v1 fixture.

Related: #2.

## P0 — Define data-flow and cardinality semantics

Status: implemented.

- undeclared ordinary inputs retain single-value fan-in and ambiguous fan-in is rejected;
- `cardinality.max: null` or `max > 1` opts an input into array aggregation;
- multi-value inputs use deterministic compiled edge-id ordering;
- declared minimum cardinality and required inputs are checked against active runtime values before executor dispatch;
- defaults apply only when no active value is present;
- type compatibility remains an authoring/compiler concern.

## P0 — Retry, timeout, and cancellation policy

Next slice:

- make retries opt-in by executor policy rather than retrying every failure indiscriminately;
- distinguish transient from permanent failures;
- add per-node execution timeouts and deterministic cancellation behavior;
- ensure cancelled/failed attempts release executor-owned resources through an explicit lifecycle boundary;
- specify lifecycle events for timeout, retry exhaustion, and cancellation races.

## P1 — Configurable parallel ready-node execution

Owner-selected direction: [TECH-012](docs/technical-decisions.md) and [#24](https://github.com/moritzbrantner/workflow-runner/issues/24). This is approved architecture, not completed implementation.

- Execute independent, ready Nodes in parallel by default; never start a dependent Node before its required inputs are available.
- Use an explicit, validated, bounded worker count with a host default and per-run override. Count `1` retains the sequential reference path.
- Choose ready starts by stable compiled ordering; real asynchronous completions need not occur in deterministic time order.
- Preserve fan-in ordering, branch/output semantics, bounded streaming and existing error/cancellation and execution-budget controls.
- Keep scheduling within the existing Runner, not a second worker service.

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

## Cross-repository ComfyUI execution reference

The owner's ComfyUI-first [asset-tooling decision](https://github.com/moritzbrantner/asset-tooling/blob/main/docs/decisions/atlas.md) keeps native ComfyUI media graphs, their inference, queue, cache and internal node execution inside ComfyUI. The asset-tooling/host adapter exposes an entire ComfyUI workflow as **one registered external node executor**, through the same compiled-workflow and `AssetRef` contracts as other operations. There is no ComfyUI-specific scheduler, graph interpreter or model-node registry in runner core.

The runner still owns generic dispatch, node lifecycle, cancellation, timeout, failure classification, and conservative retry/idempotence at this external-job boundary ([#8](https://github.com/moritzbrantner/workflow-runner/issues/8)). Inference transport must not blindly retry an ambiguous submitted job or globally interrupt an unrelated ComfyUI job. When binary results are exchanged, follow the agreed buffered/streamed boundary ([#12](https://github.com/moritzbrantner/workflow-runner/issues/12)) instead of persisting raw binary through workflow-engine. Future internal asset-tooling providers share the same operation-facing execution contract rather than requiring a parallel runner.
