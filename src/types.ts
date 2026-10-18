export type ExecutableWorkflowPortCardinality = {
  min?: number;
  max?: number | null;
};

export type ExecutableWorkflowPort = {
  id: string;
  type: unknown;
  optional?: boolean;
  defaultValue?: unknown;
  cardinality?: ExecutableWorkflowPortCardinality;
};

export type ExecutableWorkflowNode = {
  id: string;
  label?: string;
  kind: string;
  inputs?: ExecutableWorkflowPort[];
  outputs?: ExecutableWorkflowPort[];
  data?: Record<string, unknown>;
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
  version: 1;
  nodes: ExecutableWorkflowNode[];
  edges: ExecutableWorkflowEdge[];
  order: string[];
};

export type WorkflowValidationDiagnosticCode =
  | "invalid-shape"
  | "unsupported-format"
  | "unsupported-version"
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
};

export type WorkflowNodeExecutionResult = {
  outputs?: Record<string, unknown>;
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

export type WorkflowRunError = {
  code:
    | "invalid-workflow"
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
  cause?: unknown;
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
  | { type: "run.started"; runId: string; timestamp: string }
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
  | { type: "run.cancelled"; runId: string; timestamp: string };

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
    };

export type WorkflowRunnerOptions = {
  executors?: Readonly<Record<string, WorkflowNodeExecutor | WorkflowExecutorRegistration>>;
  now?: () => Date;
  timers?: WorkflowRunnerTimers;
};

export type WorkflowRunner = {
  registerExecutor(
    kind: string,
    executor: WorkflowNodeExecutor,
    policy?: WorkflowExecutorPolicy,
  ): void;
  dispatch(request: WorkflowRunRequest): Promise<WorkflowRunResult>;
};
