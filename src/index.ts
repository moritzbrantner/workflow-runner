export type ExecutableWorkflowPort = {
  id: string;
  optional?: boolean;
  defaultValue?: unknown;
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

export type WorkflowRunError = {
  code: "invalid-workflow" | "missing-executor" | "node-failed";
  message: string;
  nodeId?: string;
  cause?: unknown;
};

export type WorkflowRunEvent =
  | { type: "run.started"; runId: string; timestamp: string }
  | { type: "node.started"; runId: string; nodeId: string; attempt: number; timestamp: string }
  | {
      type: "node.retrying";
      runId: string;
      nodeId: string;
      attempt: number;
      timestamp: string;
      error: WorkflowRunError;
    }
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
  maxAttempts?: number;
  now?: () => Date;
};

export type WorkflowRunner = {
  registerExecutor(kind: string, executor: WorkflowNodeExecutor): void;
  dispatch(request: WorkflowRunRequest): Promise<WorkflowRunResult>;
};

const hasOwn = (value: object, key: PropertyKey): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function fail(
  code: WorkflowRunError["code"],
  message: string,
  nodeId?: string,
  cause?: unknown,
): WorkflowRunError {
  return {
    code,
    message,
    ...(nodeId ? { nodeId } : {}),
    ...(cause === undefined ? {} : { cause }),
  };
}

function validateWorkflow(workflow: ExecutableWorkflow): WorkflowRunError | null {
  if (workflow.format !== "@moritzbrantner/workflow/compiled" || workflow.version !== 1) {
    return fail("invalid-workflow", "Unsupported compiled workflow format or version.");
  }

  const nodeIds = new Set(workflow.nodes.map((node) => node.id));
  if (nodeIds.size !== workflow.nodes.length || workflow.order.length !== workflow.nodes.length) {
    return fail(
      "invalid-workflow",
      "Compiled workflow must contain unique nodes and a complete order.",
    );
  }

  if (
    new Set(workflow.order).size !== workflow.order.length ||
    workflow.order.some((id) => !nodeIds.has(id))
  ) {
    return fail(
      "invalid-workflow",
      "Compiled workflow order must contain each node exactly once.",
    );
  }

  const orderIndex = new Map(workflow.order.map((id, index) => [id, index] as const));
  for (const edge of workflow.edges) {
    const sourceIndex = orderIndex.get(edge.sourceNodeId);
    const targetIndex = orderIndex.get(edge.targetNodeId);
    if (sourceIndex === undefined || targetIndex === undefined || sourceIndex >= targetIndex) {
      return fail("invalid-workflow", `Edge ${edge.id} violates the compiled DAG order.`);
    }
  }

  return null;
}

function buildInputs(
  node: ExecutableWorkflowNode,
  incomingEdges: readonly ExecutableWorkflowEdge[],
  nodeResults: Readonly<Record<string, WorkflowNodeResult>>,
): { inputs: Record<string, unknown>; activeEdgeCount: number } {
  const inputs: Record<string, unknown> = {};
  let activeEdgeCount = 0;

  for (const edge of incomingEdges) {
    const sourceResult = nodeResults[edge.sourceNodeId];
    if (
      sourceResult?.status !== "succeeded" ||
      !hasOwn(sourceResult.outputs, edge.sourcePortId)
    ) {
      continue;
    }
    inputs[edge.targetPortId] = sourceResult.outputs[edge.sourcePortId];
    activeEdgeCount += 1;
  }

  for (const port of node.inputs ?? []) {
    if (!hasOwn(inputs, port.id) && hasOwn(port, "defaultValue")) {
      inputs[port.id] = port.defaultValue;
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
    return result?.status === "succeeded"
      ? [{ nodeId: node.id, outputs: result.outputs }]
      : [];
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

function createBuiltInExecutors(): Record<string, WorkflowNodeExecutor> {
  return {
    "control.start": ({ workflowInput }) => ({ outputs: { out: workflowInput } }),
    "control.end": ({ inputs }) => ({ outputs: { result: inputs.in } }),
    "control.if": ({ inputs }) => ({
      outputs: inputs.condition ? { true: inputs.value } : { false: inputs.value },
    }),
    "control.switch": ({ inputs }) => ({
      outputs: Object.is(inputs.value, inputs.case)
        ? { match: inputs.value }
        : { default: inputs.value },
    }),
    "control.merge": ({ inputs }) => ({
      outputs: { out: hasOwn(inputs, "a") ? inputs.a : inputs.b },
    }),
    "json.string": ({ node }) => ({ outputs: { value: node.data?.value ?? "" } }),
    "json.number": ({ node }) => ({ outputs: { value: node.data?.value ?? 0 } }),
    "json.boolean": ({ node }) => ({ outputs: { value: node.data?.value ?? false } }),
    "json.null": () => ({ outputs: { value: null } }),
  };
}

export function createWorkflowRunner(options: WorkflowRunnerOptions = {}): WorkflowRunner {
  const executors = new Map<string, WorkflowNodeExecutor>(
    Object.entries({ ...createBuiltInExecutors(), ...(options.executors ?? {}) }),
  );
  const maxAttempts = Math.max(1, Math.floor(options.maxAttempts ?? 1));
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
      const invalid = validateWorkflow(request.workflow);

      if (invalid) {
        emit({
          type: "run.failed",
          runId: request.runId,
          timestamp: timestamp(),
          error: invalid,
        });
        return { status: "failed", error: invalid, nodeResults, events };
      }

      emit({ type: "run.started", runId: request.runId, timestamp: timestamp() });

      const nodeById = new Map(request.workflow.nodes.map((node) => [node.id, node] as const));
      const incomingByNodeId = new Map<string, ExecutableWorkflowEdge[]>();
      for (const edge of request.workflow.edges) {
        const incoming = incomingByNodeId.get(edge.targetNodeId) ?? [];
        incoming.push(edge);
        incomingByNodeId.set(edge.targetNodeId, incoming);
      }

      for (const nodeId of request.workflow.order) {
        if (request.signal?.aborted) {
          emit({ type: "run.cancelled", runId: request.runId, timestamp: timestamp() });
          return { status: "cancelled", nodeResults, events };
        }

        const node = nodeById.get(nodeId);
        if (!node) {
          const error = fail(
            "invalid-workflow",
            `Compiled workflow is missing node ${nodeId}.`,
            nodeId,
          );
          emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
          return { status: "failed", error, nodeResults, events };
        }

        const incomingEdges = incomingByNodeId.get(nodeId) ?? [];
        const { inputs, activeEdgeCount } = buildInputs(node, incomingEdges, nodeResults);
        if (incomingEdges.length > 0 && activeEdgeCount === 0) {
          nodeResults[nodeId] = {
            status: "skipped",
            attempts: 0,
            inputs,
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

        const executor = executors.get(node.kind);
        if (!executor) {
          const error = fail(
            "missing-executor",
            `No executor is registered for workflow node kind ${node.kind}.`,
            nodeId,
          );
          nodeResults[nodeId] = {
            status: "failed",
            attempts: 0,
            inputs,
            outputs: {},
            error,
          };
          emit({ type: "run.failed", runId: request.runId, timestamp: timestamp(), error });
          return { status: "failed", error, nodeResults, events };
        }

        let lastError: WorkflowRunError | null = null;
        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
          emit({
            type: "node.started",
            runId: request.runId,
            nodeId,
            attempt,
            timestamp: timestamp(),
          });
          try {
            const result = await executor({
              runId: request.runId,
              node,
              inputs,
              workflowInput: request.input ?? {},
              context: request.context ?? {},
              ...(request.signal ? { signal: request.signal } : {}),
            });
            const outputs = result.outputs ?? {};
            nodeResults[nodeId] = { status: "succeeded", attempts: attempt, inputs, outputs };
            emit({
              type: "node.succeeded",
              runId: request.runId,
              nodeId,
              attempt,
              timestamp: timestamp(),
              outputs,
            });
            lastError = null;
            break;
          } catch (cause) {
            lastError = fail(
              "node-failed",
              `Node ${nodeId} failed: ${toErrorMessage(cause)}`,
              nodeId,
              cause,
            );
            if (attempt < maxAttempts) {
              emit({
                type: "node.retrying",
                runId: request.runId,
                nodeId,
                attempt,
                timestamp: timestamp(),
                error: lastError,
              });
            }
          }
        }

        if (lastError) {
          nodeResults[nodeId] = {
            status: "failed",
            attempts: maxAttempts,
            inputs,
            outputs: {},
            error: lastError,
          };
          emit({
            type: "node.failed",
            runId: request.runId,
            nodeId,
            attempt: maxAttempts,
            timestamp: timestamp(),
            error: lastError,
          });
          emit({
            type: "run.failed",
            runId: request.runId,
            timestamp: timestamp(),
            error: lastError,
          });
          return { status: "failed", error: lastError, nodeResults, events };
        }
      }

      const output = resolveRunOutput(request.workflow, nodeResults);
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
