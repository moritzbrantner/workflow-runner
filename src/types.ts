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
  inputs: Readonly<Record<string, unknown>>;
  workflowInput: Readonly<Record<string, unknown>>;
  context: Readonly<Record<string, unknown>>;
  signal?: AbortSignal;
};

export type WorkflowNodeExecutor = (
  context: WorkflowNodeExecutorContext,
) => WorkflowNodeExecutionResult | Promise<WorkflowNodeExecutionResult>;

export type WorkflowRunError = {
  code: "invalid-workflow" | "input-not-ready" | "missing-executor" | "node-failed";
  message: string;
  nodeId?: string;
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
  executors?: Readonly<Record<string, WorkflowNodeExecutor>>;
  now?: () => Date;
};

export type WorkflowRunner = {
  registerExecutor(kind: string, executor: WorkflowNodeExecutor): void;
  dispatch(request: WorkflowRunRequest): Promise<WorkflowRunResult>;
};
