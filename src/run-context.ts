import type {
  ExecutableWorkflow,
  ExecutableWorkflowDefinition,
  WorkflowNodeResult,
  WorkflowRunCheckpoint,
  WorkflowRunError,
  WorkflowRunEvent,
  WorkflowRunRequest,
  WorkflowRunResult,
  WorkflowRunnerTimers,
} from "./types.js";

export type SharedRunState = {
  /** Remaining node execution attempts for the whole run, including nested work (TECH-007). */
  budget: { remaining: number };
  /** Identity of the top-level run for node result records (TECH-011). */
  persistence?: {
    workflowDigest: string;
    inputDigest: string;
    iteration?: { nodeId: string; iteration: number };
  };
  /** Pre-completed node results injected into a nested graph (iteration boundary values). */
  seed?: Record<string, WorkflowNodeResult>;
  /** Version-3 closure of the root workflow, shared by every nested reference run. */
  workflows?: Record<string, ExecutableWorkflowDefinition>;
  /** Workflow keys of the active reference invocations, outermost first. */
  callStack?: readonly string[];
};

export type NodeOutcome =
  | { kind: "succeeded" }
  | { kind: "failed"; error: WorkflowRunError }
  | { kind: "cancelled" }
  | { kind: "suspended"; checkpointWaiting: WorkflowRunCheckpoint["waiting"] };

/** What node execution needs from the run it belongs to. */
export type RunContext = {
  request: WorkflowRunRequest;
  workflow: ExecutableWorkflow;
  shared: SharedRunState;
  maxWorkers: number;
  nodeResults: Record<string, WorkflowNodeResult>;
  timers: WorkflowRunnerTimers;
  runSignal: AbortSignal;
  emit: (event: WorkflowRunEvent) => void;
  timestamp: () => string;
  /** Digest of this run's compiled workflow, which is immutable, computed at most once. */
  workflowDigest: () => string;
  /** Runs a nested graph under this runner (iteration bodies and Workflow References). */
  runNested: (request: WorkflowRunRequest, shared: SharedRunState) => Promise<WorkflowRunResult>;
};

/** Records a node's failure and emits `node.failed`; `attempt` is both its attempts and event. */
export function recordNodeFailure(
  { request, nodeResults, emit, timestamp }: RunContext,
  nodeId: string,
  attempt: number,
  inputs: Record<string, unknown>,
  error: WorkflowRunError,
): void {
  nodeResults[nodeId] = { status: "failed", attempts: attempt, inputs, outputs: {}, error };
  emit({
    type: "node.failed",
    runId: request.runId,
    nodeId,
    attempt,
    timestamp: timestamp(),
    error,
  });
}

/** Ends an iteration or reference node, whose one logical attempt ran a nested graph. */
export function failNestedNode(
  context: RunContext,
  nodeId: string,
  inputs: Record<string, unknown>,
  error: WorkflowRunError,
): NodeOutcome {
  recordNodeFailure(context, nodeId, 1, inputs, error);
  return error.code === "node-cancelled" ? { kind: "cancelled" } : { kind: "failed", error };
}

/** The request for a nested graph; it shares the parent's input, context, signal and limits. */
export function nestedRunRequest(
  { request, runSignal, maxWorkers }: RunContext,
  workflow: ExecutableWorkflow,
): WorkflowRunRequest {
  return {
    runId: request.runId,
    workflow,
    ...(request.input ? { input: request.input } : {}),
    ...(request.context ? { context: request.context } : {}),
    signal: runSignal,
    maxWorkers,
    ...(request.persistNodeResult ? { persistNodeResult: request.persistNodeResult } : {}),
  };
}
