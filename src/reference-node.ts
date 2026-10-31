import { digestExecutableWorkflow } from "./checkpoint.js";
import { runInputDigest } from "./evidence.js";
import { hasOwn } from "./node-data.js";
import { persistIfRequested } from "./node-execution.js";
import {
  failNestedNode,
  nestedRunRequest,
  type NodeOutcome,
  type RunContext,
} from "./run-context.js";
import { fail } from "./run-errors.js";
import { summarizeOutputs } from "./stream.js";
import type { ExecutableWorkflowNode, WorkflowNodeResult, WorkflowRunError } from "./types.js";

/**
 * Runs a version-3 Workflow Reference (editor TECH-005): the node's inputs feed the referenced
 * definition's interface inputs and its interface outputs become the node's outputs. maxDepth
 * bounds how many invocations of that definition may be nested (the outermost counts as 1).
 */
export async function executeReference(
  context: RunContext,
  node: ExecutableWorkflowNode,
  reference: NonNullable<ExecutableWorkflowNode["workflowRef"]>,
  inputs: Record<string, unknown>,
): Promise<NodeOutcome> {
  const { request, workflow, shared, nodeResults, emit, timestamp } = context;
  const nodeId = node.id;
  const finish = (error: WorkflowRunError) => failNestedNode(context, nodeId, inputs, error);
  const workflows = shared.workflows ?? workflow.workflows ?? {};
  const definition = workflows[reference.workflow];
  const callStack = shared.callStack ?? [];
  const depth = callStack.filter((key) => key === reference.workflow).length + 1;
  if (!definition) {
    return finish(
      fail("node-failed", `Workflow ${reference.workflow} is not in the compiled closure.`, {
        nodeId,
      }),
    );
  }
  if (reference.maxDepth !== undefined && depth > reference.maxDepth) {
    return finish(
      fail(
        "reference-depth-exhausted",
        `Node ${nodeId} would nest ${reference.workflow} ${depth} deep; maxDepth is ${reference.maxDepth}.`,
        { nodeId },
      ),
    );
  }
  emit({
    type: "node.started",
    runId: request.runId,
    nodeId,
    attempt: 1,
    timestamp: timestamp(),
  });
  const seed: Record<string, WorkflowNodeResult> = {};
  for (const port of definition.interface.inputs) {
    if (!hasOwn(inputs, port.id)) {
      continue;
    }
    const entry = seed[port.nodeId] ?? {
      status: "succeeded" as const,
      attempts: 0,
      inputs: {},
      outputs: {},
    };
    (entry.outputs as Record<string, unknown>)[port.portId] = inputs[port.id];
    seed[port.nodeId] = entry;
  }
  const result = await context.runNested(
    nestedRunRequest(context, {
      format: workflow.format,
      version: 3,
      nodes: definition.nodes,
      edges: definition.edges,
      order: definition.order,
      workflows,
    }),
    {
      budget: shared.budget,
      workflows,
      callStack: [...callStack, reference.workflow],
      seed,
      persistence: shared.persistence ?? {
        workflowDigest: digestExecutableWorkflow(workflow),
        inputDigest: runInputDigest(request),
      },
    },
  );
  if (result.status === "cancelled") {
    return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
  }
  if (result.status === "suspended") {
    return finish(
      fail(
        "node-failed",
        `Node ${nodeId}: suspension inside a referenced workflow is not supported.`,
        {
          nodeId,
        },
      ),
    );
  }
  if (result.status === "failed") {
    const passThrough = ["reference-depth-exhausted", "budget-exhausted"] as const;
    const code = (passThrough as readonly string[]).includes(result.error.code)
      ? result.error.code
      : "node-failed";
    return finish(
      fail(code, `Node ${nodeId} (${reference.workflow}) failed: ${result.error.message}`, {
        nodeId,
        attempt: 1,
        cause: result.error,
      }),
    );
  }
  const outputs: Record<string, unknown> = {};
  for (const port of definition.interface.outputs) {
    const end = result.nodeResults[port.nodeId];
    if (end?.status === "succeeded" && hasOwn(end.inputs, port.portId)) {
      outputs[port.id] = end.inputs[port.portId];
    }
  }
  const succeeded = { status: "succeeded" as const, attempts: 1, inputs, outputs };
  const persistError = await persistIfRequested(context, node, succeeded);
  if (persistError) {
    return finish(persistError);
  }
  nodeResults[nodeId] = succeeded;
  emit({
    type: "node.succeeded",
    runId: request.runId,
    nodeId,
    attempt: 1,
    timestamp: timestamp(),
    outputs: summarizeOutputs(outputs),
  });
  return { kind: "succeeded" };
}
