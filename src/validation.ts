import type {
  ExecutableWorkflow,
  ExecutableWorkflowEdge,
  ExecutableWorkflowNode,
  ExecutableWorkflowPort,
  ExecutableWorkflowPortCardinality,
  WorkflowValidationDiagnostic,
} from "./types.js";

const compiledFormat = "@moritzbrantner/workflow/compiled";
const compiledVersion = 1;

function compareStrings(left: string, right: string): number {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function pushShape(
  diagnostics: WorkflowValidationDiagnostic[],
  path: string,
  message: string,
  details: Partial<WorkflowValidationDiagnostic> = {},
): void {
  diagnostics.push({ code: "invalid-shape", path, message, ...details });
}

function validateCardinality(
  value: unknown,
  path: string,
  diagnostics: WorkflowValidationDiagnostic[],
  details: Pick<WorkflowValidationDiagnostic, "nodeId" | "portId">,
): ExecutableWorkflowPortCardinality | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!isRecord(value)) {
    diagnostics.push({
      code: "invalid-cardinality",
      path,
      message: "Port cardinality must be an object.",
      ...details,
    });
    return undefined;
  }

  const min = value.min;
  const max = value.max;
  const minValid = min === undefined || (Number.isInteger(min) && Number(min) >= 0);
  const maxValid =
    max === undefined || max === null || (Number.isInteger(max) && Number(max) >= 0);

  if (!minValid) {
    diagnostics.push({
      code: "invalid-cardinality",
      path: `${path}.min`,
      message: "Cardinality min must be a non-negative integer.",
      ...details,
    });
  }
  if (!maxValid) {
    diagnostics.push({
      code: "invalid-cardinality",
      path: `${path}.max`,
      message: "Cardinality max must be a non-negative integer or null.",
      ...details,
    });
  }
  if (!minValid || !maxValid) {
    return undefined;
  }

  const normalizedMin = min === undefined ? undefined : Number(min);
  const normalizedMax = max === undefined || max === null ? max : Number(max);
  if (
    normalizedMin !== undefined &&
    normalizedMax !== undefined &&
    normalizedMax !== null &&
    normalizedMin > normalizedMax
  ) {
    diagnostics.push({
      code: "invalid-cardinality",
      path,
      message: "Cardinality min cannot be greater than max.",
      ...details,
    });
  }

  return {
    ...(normalizedMin === undefined ? {} : { min: normalizedMin }),
    ...(normalizedMax === undefined ? {} : { max: normalizedMax }),
  };
}

function validatePorts(
  value: unknown,
  path: string,
  nodeId: string,
  diagnostics: WorkflowValidationDiagnostic[],
): ExecutableWorkflowPort[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value)) {
    pushShape(diagnostics, path, "Workflow ports must be an array.", { nodeId });
    return undefined;
  }

  const ports: ExecutableWorkflowPort[] = [];
  const seen = new Set<string>();
  value.forEach((rawPort, index) => {
    const portPath = `${path}[${index}]`;
    if (!isRecord(rawPort)) {
      pushShape(diagnostics, portPath, "Workflow port must be an object.", { nodeId });
      return;
    }
    if (!nonEmptyString(rawPort.id)) {
      pushShape(diagnostics, `${portPath}.id`, "Workflow port id must be a non-empty string.", {
        nodeId,
      });
      return;
    }
    const portId = rawPort.id;
    if (!Object.prototype.hasOwnProperty.call(rawPort, "type")) {
      pushShape(diagnostics, `${portPath}.type`, "Compiled workflow port type metadata is required.", {
        nodeId,
        portId,
      });
    }
    if (seen.has(portId)) {
      diagnostics.push({
        code: "duplicate-port-id",
        path: `${portPath}.id`,
        message: `Workflow node ${nodeId} declares duplicate port ${portId}.`,
        nodeId,
        portId,
      });
      return;
    }
    seen.add(portId);

    if (rawPort.optional !== undefined && typeof rawPort.optional !== "boolean") {
      pushShape(diagnostics, `${portPath}.optional`, "Port optional must be a boolean.", {
        nodeId,
        portId,
      });
    }
    const cardinality = validateCardinality(
      rawPort.cardinality,
      `${portPath}.cardinality`,
      diagnostics,
      { nodeId, portId },
    );
    ports.push({
      id: portId,
      type: rawPort.type,
      ...(rawPort.optional === undefined || typeof rawPort.optional !== "boolean"
        ? {}
        : { optional: rawPort.optional }),
      ...(Object.prototype.hasOwnProperty.call(rawPort, "defaultValue")
        ? { defaultValue: rawPort.defaultValue }
        : {}),
      ...(cardinality === undefined ? {} : { cardinality }),
    });
  });
  return ports;
}

function validateNodes(
  value: unknown,
  diagnostics: WorkflowValidationDiagnostic[],
): ExecutableWorkflowNode[] | null {
  if (!Array.isArray(value)) {
    pushShape(diagnostics, "nodes", "Compiled workflow nodes must be an array.");
    return null;
  }
  const nodes: ExecutableWorkflowNode[] = [];
  value.forEach((rawNode, index) => {
    const path = `nodes[${index}]`;
    if (!isRecord(rawNode)) {
      pushShape(diagnostics, path, "Workflow node must be an object.");
      return;
    }
    if (!nonEmptyString(rawNode.id)) {
      pushShape(diagnostics, `${path}.id`, "Workflow node id must be a non-empty string.");
      return;
    }
    if (!nonEmptyString(rawNode.kind)) {
      pushShape(diagnostics, `${path}.kind`, "Workflow node kind must be a non-empty string.", {
        nodeId: rawNode.id,
      });
      return;
    }
    if (rawNode.label !== undefined && typeof rawNode.label !== "string") {
      pushShape(diagnostics, `${path}.label`, "Workflow node label must be a string.", {
        nodeId: rawNode.id,
      });
    }
    if (rawNode.data !== undefined && !isRecord(rawNode.data)) {
      pushShape(diagnostics, `${path}.data`, "Workflow node data must be an object.", {
        nodeId: rawNode.id,
      });
    }
    const inputs = validatePorts(rawNode.inputs, `${path}.inputs`, rawNode.id, diagnostics);
    const outputs = validatePorts(rawNode.outputs, `${path}.outputs`, rawNode.id, diagnostics);
    nodes.push({
      id: rawNode.id,
      kind: rawNode.kind,
      ...(typeof rawNode.label === "string" ? { label: rawNode.label } : {}),
      ...(inputs === undefined ? {} : { inputs }),
      ...(outputs === undefined ? {} : { outputs }),
      ...(isRecord(rawNode.data) ? { data: rawNode.data } : {}),
    });
  });
  return nodes;
}

function validateEdges(
  value: unknown,
  diagnostics: WorkflowValidationDiagnostic[],
): ExecutableWorkflowEdge[] | null {
  if (!Array.isArray(value)) {
    pushShape(diagnostics, "edges", "Compiled workflow edges must be an array.");
    return null;
  }
  const fields = ["id", "sourceNodeId", "sourcePortId", "targetNodeId", "targetPortId"] as const;
  const edges: ExecutableWorkflowEdge[] = [];
  value.forEach((rawEdge, index) => {
    const path = `edges[${index}]`;
    if (!isRecord(rawEdge)) {
      pushShape(diagnostics, path, "Workflow edge must be an object.");
      return;
    }
    let valid = true;
    for (const field of fields) {
      if (!nonEmptyString(rawEdge[field])) {
        pushShape(diagnostics, `${path}.${field}`, `Workflow edge ${field} must be a non-empty string.`);
        valid = false;
      }
    }
    if (!valid) {
      return;
    }
    edges.push({
      id: rawEdge.id as string,
      sourceNodeId: rawEdge.sourceNodeId as string,
      sourcePortId: rawEdge.sourcePortId as string,
      targetNodeId: rawEdge.targetNodeId as string,
      targetPortId: rawEdge.targetPortId as string,
    });
  });
  return edges;
}

function validateOrder(value: unknown, diagnostics: WorkflowValidationDiagnostic[]): string[] | null {
  if (!Array.isArray(value)) {
    pushShape(diagnostics, "order", "Compiled workflow order must be an array.");
    return null;
  }
  const order: string[] = [];
  value.forEach((rawId, index) => {
    if (!nonEmptyString(rawId)) {
      pushShape(diagnostics, `order[${index}]`, "Workflow order entries must be non-empty strings.");
      return;
    }
    order.push(rawId);
  });
  return order;
}

function maxForInput(port: ExecutableWorkflowPort): number | null {
  if (port.cardinality?.max === null) {
    return null;
  }
  return port.cardinality?.max ?? 1;
}

function maxForOutput(port: ExecutableWorkflowPort): number | null {
  if (port.cardinality?.max === null) {
    return null;
  }
  return port.cardinality?.max ?? null;
}

function validateSemantics(workflow: ExecutableWorkflow): WorkflowValidationDiagnostic[] {
  const diagnostics: WorkflowValidationDiagnostic[] = [];
  const nodeById = new Map<string, ExecutableWorkflowNode>();
  for (let index = 0; index < workflow.nodes.length; index += 1) {
    const node = workflow.nodes[index];
    if (!node) {
      continue;
    }
    if (nodeById.has(node.id)) {
      diagnostics.push({
        code: "duplicate-node-id",
        path: `nodes[${index}].id`,
        message: `Compiled workflow contains duplicate node id ${node.id}.`,
        nodeId: node.id,
      });
    } else {
      nodeById.set(node.id, node);
    }
  }

  const edgeIds = new Set<string>();
  const connectionKeys = new Set<string>();
  for (let index = 0; index < workflow.edges.length; index += 1) {
    const edge = workflow.edges[index];
    if (!edge) {
      continue;
    }
    if (edgeIds.has(edge.id)) {
      diagnostics.push({
        code: "duplicate-edge-id",
        path: `edges[${index}].id`,
        message: `Compiled workflow contains duplicate edge id ${edge.id}.`,
        edgeId: edge.id,
      });
    } else {
      edgeIds.add(edge.id);
    }
    const connectionKey = `${edge.sourceNodeId}\u0000${edge.sourcePortId}\u0000${edge.targetNodeId}\u0000${edge.targetPortId}`;
    if (connectionKeys.has(connectionKey)) {
      diagnostics.push({
        code: "duplicate-edge-connection",
        path: `edges[${index}]`,
        message: `Compiled workflow repeats connection ${edge.sourceNodeId}.${edge.sourcePortId} -> ${edge.targetNodeId}.${edge.targetPortId}.`,
        edgeId: edge.id,
      });
    } else {
      connectionKeys.add(connectionKey);
    }

    const source = nodeById.get(edge.sourceNodeId);
    const target = nodeById.get(edge.targetNodeId);
    if (!source) {
      diagnostics.push({
        code: "missing-node",
        path: `edges[${index}].sourceNodeId`,
        message: `Edge ${edge.id} references missing source node ${edge.sourceNodeId}.`,
        edgeId: edge.id,
        nodeId: edge.sourceNodeId,
      });
    }
    if (!target) {
      diagnostics.push({
        code: "missing-node",
        path: `edges[${index}].targetNodeId`,
        message: `Edge ${edge.id} references missing target node ${edge.targetNodeId}.`,
        edgeId: edge.id,
        nodeId: edge.targetNodeId,
      });
    }
    if (source && !(source.outputs ?? []).some((port) => port.id === edge.sourcePortId)) {
      diagnostics.push({
        code: "missing-port",
        path: `edges[${index}].sourcePortId`,
        message: `Edge ${edge.id} references missing output port ${edge.sourceNodeId}.${edge.sourcePortId}.`,
        edgeId: edge.id,
        nodeId: edge.sourceNodeId,
        portId: edge.sourcePortId,
      });
    }
    if (target && !(target.inputs ?? []).some((port) => port.id === edge.targetPortId)) {
      diagnostics.push({
        code: "missing-port",
        path: `edges[${index}].targetPortId`,
        message: `Edge ${edge.id} references missing input port ${edge.targetNodeId}.${edge.targetPortId}.`,
        edgeId: edge.id,
        nodeId: edge.targetNodeId,
        portId: edge.targetPortId,
      });
    }
  }

  const orderSeen = new Set<string>();
  workflow.order.forEach((nodeId, index) => {
    if (orderSeen.has(nodeId)) {
      diagnostics.push({
        code: "invalid-order",
        path: `order[${index}]`,
        message: `Compiled workflow order repeats node ${nodeId}.`,
        nodeId,
      });
    }
    orderSeen.add(nodeId);
    if (!nodeById.has(nodeId)) {
      diagnostics.push({
        code: "invalid-order",
        path: `order[${index}]`,
        message: `Compiled workflow order references missing node ${nodeId}.`,
        nodeId,
      });
    }
  });
  if (workflow.order.length !== workflow.nodes.length) {
    diagnostics.push({
      code: "invalid-order",
      path: "order",
      message: "Compiled workflow order must contain each node exactly once.",
    });
  }
  for (const node of workflow.nodes) {
    if (!orderSeen.has(node.id)) {
      diagnostics.push({
        code: "invalid-order",
        path: "order",
        message: `Compiled workflow order is missing node ${node.id}.`,
        nodeId: node.id,
      });
    }
  }

  const orderIndex = new Map(workflow.order.map((id, index) => [id, index] as const));
  for (let index = 0; index < workflow.edges.length; index += 1) {
    const edge = workflow.edges[index];
    if (!edge) {
      continue;
    }
    const sourceIndex = orderIndex.get(edge.sourceNodeId);
    const targetIndex = orderIndex.get(edge.targetNodeId);
    if (sourceIndex !== undefined && targetIndex !== undefined && sourceIndex >= targetIndex) {
      diagnostics.push({
        code: "invalid-order",
        path: `edges[${index}]`,
        message: `Edge ${edge.id} violates the compiled DAG order.`,
        edgeId: edge.id,
      });
    }
  }

  if (diagnostics.some((diagnostic) => diagnostic.code === "missing-node" || diagnostic.code === "missing-port")) {
    return diagnostics;
  }

  for (const node of workflow.nodes) {
    for (const port of node.inputs ?? []) {
      const count = workflow.edges.filter(
        (edge) => edge.targetNodeId === node.id && edge.targetPortId === port.id,
      ).length;
      const min = port.cardinality?.min ?? 0;
      const max = maxForInput(port);
      if (count < min || (max !== null && count > max)) {
        diagnostics.push({
          code: "cardinality-violation",
          path: `nodes.${node.id}.inputs.${port.id}.cardinality`,
          message: `Input port ${node.id}.${port.id} accepts ${min}..${max === null ? "many" : max} connections; found ${count}.`,
          nodeId: node.id,
          portId: port.id,
        });
      }
    }
    for (const port of node.outputs ?? []) {
      const count = workflow.edges.filter(
        (edge) => edge.sourceNodeId === node.id && edge.sourcePortId === port.id,
      ).length;
      const min = port.cardinality?.min ?? 0;
      const max = maxForOutput(port);
      if (count < min || (max !== null && count > max)) {
        diagnostics.push({
          code: "cardinality-violation",
          path: `nodes.${node.id}.outputs.${port.id}.cardinality`,
          message: `Output port ${node.id}.${port.id} accepts ${min}..${max === null ? "many" : max} connections; found ${count}.`,
          nodeId: node.id,
          portId: port.id,
        });
      }
    }
  }

  return diagnostics;
}

export function validateExecutableWorkflow(value: unknown): WorkflowValidationDiagnostic[] {
  const diagnostics: WorkflowValidationDiagnostic[] = [];
  if (!isRecord(value)) {
    return [
      {
        code: "invalid-shape",
        path: "$",
        message: "Compiled workflow must be an object.",
      },
    ];
  }

  if (value.format !== compiledFormat) {
    diagnostics.push({
      code: "unsupported-format",
      path: "format",
      message: `Unsupported compiled workflow format ${String(value.format)}.`,
    });
  }
  if (value.version !== compiledVersion) {
    diagnostics.push({
      code: "unsupported-version",
      path: "version",
      message: `Unsupported compiled workflow version ${String(value.version)}.`,
    });
  }

  const nodes = validateNodes(value.nodes, diagnostics);
  const edges = validateEdges(value.edges, diagnostics);
  const order = validateOrder(value.order, diagnostics);
  if (diagnostics.length > 0 || nodes === null || edges === null || order === null) {
    return diagnostics;
  }

  const workflow: ExecutableWorkflow = {
    format: compiledFormat,
    version: compiledVersion,
    nodes,
    edges,
    order,
  };
  return validateSemantics(workflow).sort((left, right) => {
    const pathOrder = compareStrings(left.path, right.path);
    if (pathOrder !== 0) {
      return pathOrder;
    }
    return compareStrings(left.code, right.code);
  });
}
