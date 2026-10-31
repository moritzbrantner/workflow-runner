import { fail } from "./run-errors.js";
import type {
  ExecutableWorkflow,
  ExecutableWorkflowEdge,
  ExecutableWorkflowNode,
  ExecutableWorkflowPort,
  WorkflowNodeResult,
  WorkflowRunError,
} from "./types.js";

export const hasOwn = (value: object, key: PropertyKey): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

function compareIds(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

function isMultiValuePort(port: ExecutableWorkflowPort): boolean {
  if (port.cardinality === undefined) {
    return false;
  }
  const max = port.cardinality.max;
  return max === undefined || max === null || max > 1;
}

function runtimeMinimum(port: ExecutableWorkflowPort): number {
  if (port.cardinality?.min !== undefined) {
    return port.cardinality.min;
  }
  if (port.optional === true || hasOwn(port, "defaultValue")) {
    return 0;
  }
  return 1;
}

type InputBuildResult = {
  inputs: Record<string, unknown>;
  activeEdgeCount: number;
  error?: WorkflowRunError;
};

export function buildInputs(
  node: ExecutableWorkflowNode,
  incomingEdges: readonly ExecutableWorkflowEdge[],
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
  resolveValue: (edge: ExecutableWorkflowEdge, value: unknown) => unknown = (_, value) => value,
): InputBuildResult {
  const valuesByPort = new Map<string, unknown[]>();
  let activeEdgeCount = 0;

  for (const edge of [...incomingEdges].sort((left, right) => compareIds(left.id, right.id))) {
    const sourceResult = nodeResults[edge.sourceNodeId];
    if (sourceResult?.status !== "succeeded" || !hasOwn(sourceResult.outputs, edge.sourcePortId)) {
      continue;
    }
    const values = valuesByPort.get(edge.targetPortId) ?? [];
    values.push(resolveValue(edge, sourceResult.outputs[edge.sourcePortId]));
    valuesByPort.set(edge.targetPortId, values);
    activeEdgeCount += 1;
  }

  const inputs: Record<string, unknown> = {};
  for (const port of node.inputs ?? []) {
    const values = valuesByPort.get(port.id) ?? [];
    const hasDefault = hasOwn(port, "defaultValue");
    const availableCount = values.length + (values.length === 0 && hasDefault ? 1 : 0);
    const minimum = runtimeMinimum(port);
    if (availableCount < minimum) {
      return {
        inputs,
        activeEdgeCount,
        error: fail(
          "input-not-ready",
          `Node ${node.id} input ${port.id} requires at least ${minimum} active value${minimum === 1 ? "" : "s"}; found ${availableCount}.`,
          { nodeId: node.id },
        ),
      };
    }

    if (values.length > 0) {
      inputs[port.id] = isMultiValuePort(port) ? values : values[0];
      continue;
    }
    if (hasDefault) {
      inputs[port.id] = isMultiValuePort(port) ? [port.defaultValue] : port.defaultValue;
    }
  }

  return { inputs, activeEdgeCount };
}

export function resolveRunOutput(
  workflow: ExecutableWorkflow,
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
): unknown {
  const nodesWithOutgoingEdges = new Set(workflow.edges.map((edge) => edge.sourceNodeId));
  const terminalNodes = workflow.nodes.filter((node) => !nodesWithOutgoingEdges.has(node.id));
  const successful = terminalNodes.flatMap((node) => {
    const result = nodeResults[node.id];
    if (result?.status !== "succeeded") {
      return [];
    }
    return [{ nodeId: node.id, outputs: result.outputs }];
  });

  if (successful.length === 0) {
    return undefined;
  }
  if (successful.length === 1) {
    const only = successful[0];
    if (!only) {
      return undefined;
    }
    return hasOwn(only.outputs, "result") ? only.outputs.result : only.outputs;
  }

  return Object.fromEntries(
    successful.map(({ nodeId, outputs }) => [
      nodeId,
      hasOwn(outputs, "result") ? outputs.result : outputs,
    ]),
  );
}
