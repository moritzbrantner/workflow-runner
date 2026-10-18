import type { WorkflowNodeResultRecord, WorkflowRunRerun } from "./evidence.js";

export type ExecutableWorkflowPortCardinality = {
  min?: number;
  max?: number | null;
};

export type ExecutableWorkflowPortPayload = {
  /** `streamed` outputs return an AsyncIterable of chunks; `buffered` values are complete. */
  mode: "buffered" | "streamed";
  mediaType?: string;
};

export type ExecutableWorkflowPort = {
  id: string;
  type: unknown;
  optional?: boolean;
  defaultValue?: unknown;
  cardinality?: ExecutableWorkflowPortCardinality;
  /** Delivery contract from workflow-editor; absent means buffered. */
  payload?: ExecutableWorkflowPortPayload;
};

export type ExecutableWorkflowIterationBoundary = { nodeId: string; portId: string };

/**
 * A bounded loop from workflow-editor compiled version 2 (docs/compiler.md). The body runs at most
 * `maxIterations` times; each `next` output becomes the following `state`.
 */
export type ExecutableWorkflowIteration = {
  maxIterations: number;
  state: { inputPortId: "state"; type?: unknown };
  stop: { kind: "body-output"; outputId: "converged" } | { kind: "limit-only" };
  candidate: "latest-state";
  result: { outputPortId: string; statuses: string[] };
  body: {
    nodes: ExecutableWorkflowNode[];
    edges: ExecutableWorkflowEdge[];
    order: string[];
    inputs: Array<ExecutableWorkflowIterationBoundary & { id: string }>;
    outputs: {
      next: ExecutableWorkflowIterationBoundary;
      converged?: ExecutableWorkflowIterationBoundary;
    };
  };
};

export type ExecutableWorkflowNode = {
  id: string;
  label?: string;
  kind: string;
  inputs?: ExecutableWorkflowPort[];
  outputs?: ExecutableWorkflowPort[];
  data?: Record<string, unknown>;
  /** Compiled-v2 bounded iteration (`control.iterate`). */
  iteration?: ExecutableWorkflowIteration;
  /** `on-completion` asks for the completed result to be persisted immediately (TECH-011). */
  persistence?: "on-completion";
  /**
   * Compiled human task contract (workflow-editor `human.task.approval` / `human.task.input`).
   * Opaque to the runner beyond being forwarded in the suspension request; Engine validates responses.
   */
  humanTask?: unknown;
};

export type ExecutableWorkflowEdge = {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
};

export type ExecutableWorkflow = {
  format: "@moritzbrantner/workflow/compiled";
  /** 1 for plain DAGs; 2 when the workflow contains bounded iteration. */
  version: 1 | 2;
  nodes: ExecutableWorkflowNode[];
  edges: ExecutableWorkflowEdge[];
  order: string[];
};

export type WorkflowValidationDiagnosticCode =
  | "invalid-shape"
  | "unsupported-format"
  | "unsupported-version"
  | "invalid-iteration"
  | "payload-mode-mismatch"
  | "nested-iteration-not-supported"
  | "duplicate-node-id"
  | "duplicate-edge-id"
  | "duplicate-port-id"
  | "duplicate-edge-connection"
  | "missing-node"
  | "missing-port"
  | "invalid-order"
  | "invalid-cardinality"
  | "cardinality-violation";

export type WorkflowValidationDiagnostic = {
  code: WorkflowValidationDiagnosticCode;
  path: string;
  message: string;
  nodeId?: string;
  edgeId?: string;
  portId?: string;
};

export type WorkflowRunRequest = {
  runId: string;
  workflow: ExecutableWorkflow;
  input?: Record<string, unknown>;
  context?: Record<string, unknown>;
  signal?: AbortSignal;
  /**
   * Live lifecycle observer, called synchronously with each event as it happens. The same events
   * are still returned in the terminal result. Observer exceptions are ignored.
   */
  onEvent?: (event: WorkflowRunEvent) => void;
  /**
   * Host persistence for nodes compiled with `persistence: "on-completion"` (TECH-011). The runner
   * awaits it before the node counts as completed; a rejection fails the node with
   * `persistence-failed`. Without it, opted-in results persist only with the terminal result.
   */
  persistNodeResult?: (record: WorkflowNodeResultRecord) => void | Promise<void>;
  /** Simultaneous node executions for this run; overrides the runner default (TECH-012). */
  maxWorkers?: number;
  /**
   * Run-wide budget of node execution attempts, counting retries and nested iteration work
   * (TECH-006/007). Overrides the runner option; without either, attempts are unbudgeted.
   */
  maxNodeAttempts?: number;
  /**
   * Runs the selected nodes and their downstream nodes again, reusing the other recorded results
   * from a previous run's evidence when its workflow and input digests still match (#23).
   */
  rerun?: WorkflowRunRerun;
  /** Continues a suspended run from its checkpoint, delivering the waiting task's response. */
  resume?: WorkflowRunResume;
};

export type WorkflowRunResume = {
  checkpoint: WorkflowRunCheckpoint;
  response: unknown;
};

/** Pauses the run at this node until the host resumes it with a response. */
export type WorkflowNodeSuspension = {
  /** Host-meaningful task identity, unique within the run. */
  taskId: string;
  /** JSON-safe description of what the waiting task asks for. */
  request?: unknown;
};

export type WorkflowNodeExecutionResult = {
  outputs?: Record<string, unknown>;
  /** Suspends the run instead of completing the node; `outputs` is ignored. */
  suspend?: WorkflowNodeSuspension;
};

export const workflowRunCheckpointFormat = "@moritzbrantner/workflow-runner/checkpoint" as const;

/**
 * JSON-safe continuation of a suspended run (Engine TECH-001). It pins the run and the exact
 * compiled workflow, records completed node results so they are never re-executed, and names the
 * waiting node and task.
 */
export type WorkflowRunCheckpoint = {
  format: typeof workflowRunCheckpointFormat;
  version: 1;
  runId: string;
  /** `sha256:<hex>` of the canonical JSON of the compiled workflow. */
  workflowDigest: string;
  waiting: WorkflowNodeSuspension & { nodeId: string; attempt: number };
  /** Results of nodes that finished before the wait, keyed by node ID. */
  completed: Record<string, Extract<WorkflowNodeResult, { status: "succeeded" | "skipped" }>>;
};

export type WorkflowNodeExecutorContext = {
  runId: string;
  node: ExecutableWorkflowNode;
  /** 1 for the first attempt; increases only when the executor's retry policy permits. */
  attempt: number;
  inputs: Readonly<Record<string, unknown>>;
  workflowInput: Readonly<Record<string, unknown>>;
  context: Readonly<Record<string, unknown>>;
  /** Aborted when the run is cancelled or this attempt times out. */
  signal?: AbortSignal;
  /** Present when this node is resumed after suspending: the host's response to its task. */
  resume?: { taskId: string; request?: unknown; response: unknown };
};

export type WorkflowNodeExecutor = (
  context: WorkflowNodeExecutorContext,
) => WorkflowNodeExecutionResult | Promise<WorkflowNodeExecutionResult>;

/**
 * Retry policy declared by an executor registration (TECH-005). Without one, an executor runs
 * exactly once. Another attempt starts only when `isRetryable` classifies the failure as transient,
 * attempts remain, and the registration declares repeated execution safe with `idempotent: true`.
 */
export type WorkflowExecutorRetryPolicy = {
  /** Total attempts including the first: a finite integer of at least 1. */
  maxAttempts: number;
  /** Returns `true` only for transient failures that may be retried. */
  isRetryable: (error: unknown, attempt: number) => boolean;
  /** Explicit statement that repeating this executor is safe (idempotent or idempotently keyed). */
  idempotent: true;
  /** Delay in milliseconds before the next attempt; defaults to none. */
  backoffMs?: (attempt: number) => number;
};

/** Per-executor execution policy (TECH-005, TECH-008). */
export type WorkflowExecutorPolicy = {
  retry?: WorkflowExecutorRetryPolicy;
  /** Finite per-attempt timeout in milliseconds. The attempt's signal is aborted when it elapses. */
  timeoutMs?: number;
  /**
   * Terminates work that does not stop cooperatively, for executors behind a host-owned isolation
   * boundary (TECH-008). Invoked when an aborted attempt has not settled after `shutdownMs`.
   */
  terminate?: (context: WorkflowNodeExecutorContext) => void | Promise<void>;
  /** Grace period in milliseconds between aborting an attempt and calling `terminate`; default 0. */
  shutdownMs?: number;
};

export type WorkflowExecutorRegistration = {
  execute: WorkflowNodeExecutor;
  policy: WorkflowExecutorPolicy;
};

/** Timer seam so timeouts and backoff are testable without wall-clock sleeps. */
export type WorkflowRunnerTimers = {
  setTimeout(callback: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
};

/** JSON-safe description of a thrown executor exception. */
export type WorkflowErrorCause = {
  name: string;
  message: string;
  code?: string | number;
};

export type WorkflowRunError = {
  code:
    | "invalid-workflow"
    | "invalid-checkpoint"
    | "invalid-run-options"
    | "invalid-rerun"
    | "persistence-failed"
    | "budget-exhausted"
    | "iteration-no-result"
    | "invalid-stream-output"
    | "stream-consumers-exceed-workers"
    | "stream-deadlock"
    | "input-not-ready"
    | "missing-executor"
    | "node-failed"
    | "node-timed-out"
    | "node-cancelled"
    | "node-termination-failed"
    | "retries-exhausted";
  message: string;
  nodeId?: string;
  attempt?: number;
  /** JSON-safe summary of the underlying exception, or the failing nested run's error. */
  cause?: WorkflowErrorCause | WorkflowRunError;
  diagnostics?: WorkflowValidationDiagnostic[];
};

export type WorkflowNodeResult =
  | {
      status: "succeeded";
      attempts: number;
      inputs: Record<string, unknown>;
      outputs: Record<string, unknown>;
    }
  | {
      status: "skipped";
      attempts: 0;
      inputs: Record<string, unknown>;
      outputs: Record<string, never>;
      reason: "inactive-inputs";
    }
  | {
      status: "failed";
      attempts: number;
      inputs: Record<string, unknown>;
      outputs: Record<string, never>;
      error: WorkflowRunError;
    };

export type WorkflowRunEvent =
  | { type: "run.started"; runId: string; maxWorkers: number; timestamp: string }
  | { type: "node.started"; runId: string; nodeId: string; attempt: number; timestamp: string }
  | {
      type: "node.succeeded";
      runId: string;
      nodeId: string;
      attempt: number;
      timestamp: string;
      outputs: Record<string, unknown>;
    }
  | {
      type: "node.skipped";
      runId: string;
      nodeId: string;
      timestamp: string;
      reason: "inactive-inputs";
    }
  | {
      type: "node.retrying";
      runId: string;
      nodeId: string;
      attempt: number;
      nextAttempt: number;
      delayMs: number;
      reason: string;
      timestamp: string;
    }
  | {
      type: "node.failed";
      runId: string;
      nodeId: string;
      attempt: number;
      timestamp: string;
      error: WorkflowRunError;
    }
  | { type: "run.succeeded"; runId: string; timestamp: string; output: unknown }
  | { type: "run.failed"; runId: string; timestamp: string; error: WorkflowRunError }
  | { type: "run.cancelled"; runId: string; timestamp: string }
  | {
      type: "node.persisted";
      runId: string;
      nodeId: string;
      attempt: number;
      iteration?: { nodeId: string; iteration: number };
      timestamp: string;
    }
  | {
      type: "run.rerun";
      runId: string;
      fromRunId: string;
      reusedNodeIds: string[];
      rerunNodeIds: string[];
      timestamp: string;
    }
  | {
      type: "node.iteration";
      runId: string;
      nodeId: string;
      iteration: number;
      converged: boolean;
      timestamp: string;
    }
  | {
      type: "run.resumed";
      runId: string;
      nodeId: string;
      taskId: string;
      maxWorkers: number;
      timestamp: string;
    }
  | {
      type: "node.suspended";
      runId: string;
      nodeId: string;
      attempt: number;
      taskId: string;
      timestamp: string;
    }
  | { type: "run.suspended"; runId: string; timestamp: string; checkpoint: WorkflowRunCheckpoint };

export type WorkflowRunResult =
  | {
      status: "succeeded";
      output: unknown;
      nodeResults: Record<string, WorkflowNodeResult>;
      events: WorkflowRunEvent[];
    }
  | {
      status: "failed";
      error: WorkflowRunError;
      nodeResults: Record<string, WorkflowNodeResult>;
      events: WorkflowRunEvent[];
    }
  | {
      status: "cancelled";
      nodeResults: Record<string, WorkflowNodeResult>;
      events: WorkflowRunEvent[];
    }
  | {
      status: "suspended";
      checkpoint: WorkflowRunCheckpoint;
      nodeResults: Record<string, WorkflowNodeResult>;
      events: WorkflowRunEvent[];
    };

export type WorkflowRunnerOptions = {
  executors?: Readonly<Record<string, WorkflowNodeExecutor | WorkflowExecutorRegistration>>;
  now?: () => Date;
  timers?: WorkflowRunnerTimers;
  /** Default simultaneous node executions per run; a positive whole number (default 4). */
  maxWorkers?: number;
  /** Default run-wide node attempt budget; unbudgeted when omitted. */
  maxNodeAttempts?: number;
  /** Chunks buffered per stream consumer before the producer waits; default 8. */
  streamQueueCapacity?: number;
};

export type WorkflowRunner = {
  registerExecutor(
    kind: string,
    executor: WorkflowNodeExecutor,
    policy?: WorkflowExecutorPolicy,
  ): void;
  dispatch(request: WorkflowRunRequest): Promise<WorkflowRunResult>;
};
