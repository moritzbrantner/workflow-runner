import { createBuiltInExecutors } from "./builtins.js";
import type {
  ExecutableWorkflow,
  ExecutableWorkflowEdge,
  ExecutableWorkflowNode,
  ExecutableWorkflowPort,
  WorkflowNodeExecutor,
  WorkflowNodeResult,
  WorkflowRunError,
  WorkflowRunEvent,
  WorkflowRunner,
  WorkflowRunnerOptions,
} from "./types.js";
import { validateExecutableWorkflow } from "./validation.js";

const hasOwn = (value: object, key: PropertyKey): boolean =>
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

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function fail(
  code: WorkflowRunError["code"],
  message: string,
  options: Omit<WorkflowRunError, "code" | "message"> = {},
): WorkflowRunError {
  return { code, message, ...options };
}

function isMultiValuePort(port: ExecutableWorkflowPort): boolean {
  const max = port.cardinality?.max;
  return max === null || (typeof max === "number" && max > 1);
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

function buildInputs(
  node: ExecutableWorkflowNode,
  incomingEdges: readonly ExecutableWorkflowEdge[],
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
): InputBuildResult {
  const valuesByPort = new Map<string, unknown[]>();
  let activeEdgeCount = 0;

  for (const edge of [...incomingEdges].sort((left, right) => compareIds(left.id, right.id))) {
    const sourceResult = nodeResults[edge.sourceNodeId];
    if (
      sourceResult?.status !== "succeeded" ||
      !hasOwn(sourceResult.outputs, edge.sourcePortId)
    ) {
      continue;
    }
    const values = valuesByPort.get(edge.targetPortId) ?? [];
    values.push(sourceResult.outputs[edge.sourcePortId]);
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

function resolveRunOutput(
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

export function createWorkflowRunner(options: WorkflowRunnerOptions = {}): WorkflowRunner {
  const executors = new Map<string, WorkflowNodeExecutor>(
    Object.entries({ ...createBuiltInExecutors(), ...(options.executors ?? {}) }),
  );
  const now = options.now ?? (() => new Date());

  return {
    registerExecutor(kind, executor) {
      executors.set(kind, executor);
    },

    async dispatch(request) {
      const events: WorkflowRunEvent[] = [];
      const nodeResults: Record<string, WorkflowNodeResult> = {};
      const emit = (event: WorkflowRunEvent) => events.push(event);
      const timestamp = () => now().toISOString();
      const diagnostics = validateExecutableWorkflow(request.workflow as unknown);

      if (diagnostics.length > 0) {
        const error = fail(
          "invalid-workflow",
          `Compiled workflow validation failed with ${diagnostics.length} diagnostic${diagnostics.length === 1 ? "" : "s"}.`,
          { diagnostics },
        );
        emit({
          type: "run.failed",
          runId: request.runId,
          timestamp: timestamp(),
          error,
        });
        return { status: "failed", error, nodeResults, events };
      }

      emit({ type: "run.started", runId: request.runId, timestamp: timestamp() });

      const workflow = request.workflow;
      const nodeById = new Map(workflow.nodes.map((node) => [node.id, node] as const));
      const incomingByNodeId = new Map<string, ExecutableWorkflowEdge[]>();
      for (const edge of workflow.edges) {
        const incoming = incomingByNodeId.get(edge.targetNodeId) ?? [];
        incoming.push(edge);
        incomingByNodeId.set(edge.targetNodeId, incoming);
      }

      for (const nodeId of workflow.order) {
        if (request.signal?.aborted) {
          emit({ type: "run.cancelled", runId: request.runId, timestamp: timestamp() });
          return { status: "cancelled", nodeResults, events };
        }

        const node = nodeById.get(nodeId);
        if (!node) {
          throw new Error(`Validated workflow is missing node ${nodeId}.`);
        }

        const incomingEdges = incomingByNodeId.get(nodeId) ?? [];
        const built = buildInputs(node, incomingEdges, nodeResults);
        if (incomingEdges.length > 0 && built.activeEdgeCount === 0) {
          nodeResults[nodeId] = {
            status: "skipped",
            attempts: 0,
            inputs: built.inputs,
            outputs: {},
            reason: "inactive-inputs",
          };
          emit({
            type: "node.skipped",
            runId: request.runId,
            nodeId,
            timestamp: timestamp(),
            reason: "inactive-inputs",
          });
          continue;
        }
        if (built.error) {
          nodeResults[nodeId] = {
            status: "failed",
            attempts: 0,
            inputs: built.inputs,
            outputs: {},
            error: built.error,
          };
          emit({
            type: "node.failed",
            runId: request.runId,
            nodeId,
            attempt: 0,
            timestamp: timestamp(),
            error: built.error,
          });
          emit({
            type: "run.failed",
            runId: request.runId,
            timestamp: timestamp(),
            error: built.error,
          });
          return { status: "failed", error: built.error, nodeResults, events };
        }

        const executor = executors.get(node.kind);
        if (!executor) {
          const error = fail(
            "missing-executor",
            `No executor is registered for workflow node kind ${node.kind}.`,
            { nodeId },
          );
          nodeResults[nodeId] = {
            status: "failed",
            attempts: 0,
            inputs: built.inputs,
            outputs: {},
            error,
          };
          emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
          return { status: "failed", error, nodeResults, events };
        }

        emit({
          type: "node.started",
          runId: request.runId,
          nodeId,
          attempt: 1,
          timestamp: timestamp(),
        });
        try {
          const result = await executor({
            runId: request.runId,
            node,
            inputs: built.inputs,
            workflowInput: request.input ?? {},
            context: request.context ?? {},
            ...(request.signal ? { signal: request.signal } : {}),
          });
          const outputs = result.outputs ?? {};
          nodeResults[nodeId] = {
            status: "succeeded",
            attempts: 1,
            inputs: built.inputs,
            outputs,
          };
          emit({
            type: "node.succeeded",
            runId: request.runId,
            nodeId,
            attempt: 1,
            timestamp: timestamp(),
            outputs,
          });
        } catch (cause) {
          const error = fail(
            "node-failed",
            `Node ${nodeId} failed: ${toErrorMessage(cause)}`,
            { nodeId, cause },
          );
          nodeResults[nodeId] = {
            status: "failed",
            attempts: 1,
            inputs: built.inputs,
            outputs: {},
            error,
          };
          emit({
            type: "node.failed",
            runId: request.runId,
            nodeId,
            attempt: 1,
            timestamp: timestamp(),
            error,
          });
          emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
          return { status: "failed", error, nodeResults, events };
        }
      }

      const output = resolveRunOutput(workflow, nodeResults);
      emit({
        type: "run.succeeded",
        runId: request.runId,
        timestamp: timestamp(),
        output,
      });
      return { status: "succeeded", output, nodeResults, events };
    },
  };
}
