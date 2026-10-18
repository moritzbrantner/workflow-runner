import { digestExecutableWorkflow, digestJsonValue } from "./checkpoint.js";
import type {
  ExecutableWorkflow,
  WorkflowNodeResult,
  WorkflowRunRequest,
  WorkflowRunResult,
} from "./types.js";

export const workflowRunEvidenceFormat = "@moritzbrantner/workflow-runner/evidence" as const;

type ReusableResult = Extract<WorkflowNodeResult, { status: "succeeded" | "skipped" }>;

/**
 * JSON-safe record of a finished run's reusable node results (TECH-011). The host or engine stores
 * it with the run; a later selective rerun may reuse results only when the workflow digest and the
 * run input/context digest still match.
 */
export type WorkflowRunEvidence = {
  format: typeof workflowRunEvidenceFormat;
  version: 1;
  runId: string;
  workflowDigest: string;
  /** `sha256:<hex>` of the canonical `{ input, context }` of the run. */
  inputDigest: string;
  /** Succeeded and skipped node results whose inputs and outputs are JSON-safe. */
  results: Record<string, ReusableResult>;
};

export type WorkflowRunRerun = {
  evidence: WorkflowRunEvidence;
  /** Nodes to execute again; their downstream nodes are executed again too. */
  nodeIds: string[];
};

function isJsonSafe(value: unknown, seen = new Set<object>()): boolean {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return true;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) && !Object.is(value, -0);
  }
  if (typeof value !== "object" || seen.has(value)) {
    return false;
  }
  seen.add(value);
  const prototype = Object.getPrototypeOf(value);
  const safe = Array.isArray(value)
    ? value.every((item) => isJsonSafe(item, seen))
    : (prototype === Object.prototype || prototype === null) &&
      Object.values(value).every((item) => isJsonSafe(item, seen));
  seen.delete(value);
  return safe;
}

export function runInputDigest(request: Pick<WorkflowRunRequest, "input" | "context">): string {
  return digestJsonValue({ input: request.input ?? {}, context: request.context ?? {} });
}

/** Builds reusable evidence from any finished run (succeeded, failed, cancelled or suspended). */
export function createWorkflowRunEvidence(
  request: Pick<WorkflowRunRequest, "runId" | "workflow" | "input" | "context">,
  result: Pick<WorkflowRunResult, "nodeResults">,
): WorkflowRunEvidence {
  const results: Record<string, ReusableResult> = {};
  for (const [nodeId, nodeResult] of Object.entries(result.nodeResults)) {
    if (
      (nodeResult.status === "succeeded" || nodeResult.status === "skipped") &&
      isJsonSafe(nodeResult.inputs) &&
      isJsonSafe(nodeResult.outputs)
    ) {
      results[nodeId] = structuredClone(nodeResult);
    }
  }
  return {
    format: workflowRunEvidenceFormat,
    version: 1,
    runId: request.runId,
    workflowDigest: digestExecutableWorkflow(request.workflow),
    inputDigest: runInputDigest(request),
    results,
  };
}

/** Plans a selective rerun; returns the reusable results or the reason reuse is impossible. */
export function planRerun(
  rerun: WorkflowRunRerun,
  workflow: ExecutableWorkflow,
  request: Pick<WorkflowRunRequest, "input" | "context">,
): { reused: Record<string, ReusableResult>; rerunNodeIds: string[] } | { problem: string } {
  const evidence = rerun.evidence as Partial<WorkflowRunEvidence> | undefined;
  if (
    !evidence ||
    evidence.format !== workflowRunEvidenceFormat ||
    evidence.version !== 1 ||
    typeof evidence.results !== "object" ||
    evidence.results === null
  ) {
    return { problem: "Rerun evidence is missing or malformed." };
  }
  if (evidence.workflowDigest !== digestExecutableWorkflow(workflow)) {
    return {
      problem: "Recorded results belong to a different workflow version; they cannot be reused.",
    };
  }
  if (evidence.inputDigest !== runInputDigest(request)) {
    return {
      problem: "The run input or context changed since the recorded run; results cannot be reused.",
    };
  }
  const known = new Set(workflow.order);
  if (!Array.isArray(rerun.nodeIds) || rerun.nodeIds.length === 0) {
    return { problem: "Select at least one node to run again." };
  }
  const unknown = rerun.nodeIds.find((nodeId) => !known.has(nodeId));
  if (unknown !== undefined) {
    return { problem: `Selected node ${unknown} does not exist in this workflow.` };
  }

  const affected = new Set(rerun.nodeIds);
  for (const nodeId of workflow.order) {
    if (
      workflow.edges.some((edge) => edge.targetNodeId === nodeId && affected.has(edge.sourceNodeId))
    ) {
      affected.add(nodeId);
    }
  }
  const reused: Record<string, ReusableResult> = {};
  for (const [nodeId, result] of Object.entries(evidence.results)) {
    if (known.has(nodeId) && !affected.has(nodeId)) {
      reused[nodeId] = structuredClone(result);
    }
  }
  return { reused, rerunNodeIds: workflow.order.filter((nodeId) => !reused[nodeId]) };
}
