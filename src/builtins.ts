import type { ExecutableWorkflowNode, WorkflowNodeExecutor } from "./types.js";

const hasOwn = (value: object, key: PropertyKey): boolean =>
  Object.prototype.hasOwnProperty.call(value, key);

function orderedArrayInputs(
  node: ExecutableWorkflowNode,
  inputs: Readonly<Record<string, unknown>>,
): unknown[] {
  return (node.inputs ?? [])
    .filter((port) => port.id !== "item-add" && hasOwn(inputs, port.id))
    .map((port) => inputs[port.id]);
}

export function createBuiltInExecutors(): Record<string, WorkflowNodeExecutor> {
  return {
    // `out` carries the whole run input; other declared ports (a referenced workflow's interface
    // inputs, editor TECH-005) carry the run input field with the same id.
    "control.start": ({ node, workflowInput }) => ({
      outputs: Object.fromEntries([
        ["out", workflowInput],
        ...(node.outputs ?? [])
          .filter((port) => port.id !== "out" && hasOwn(workflowInput, port.id))
          .map((port) => [port.id, workflowInput[port.id]]),
      ]),
    }),
    // A legacy `in` port is the result; named end ports (interface outputs) are passed through.
    "control.end": ({ inputs }) => ({
      outputs: hasOwn(inputs, "in") ? { result: inputs.in } : { ...inputs },
    }),
    "control.if": ({ inputs }) => ({
      outputs: inputs.condition ? { true: inputs.value } : { false: inputs.value },
    }),
    "control.switch": ({ inputs }) => ({
      outputs: Object.is(inputs.value, inputs.case)
        ? { match: inputs.value }
        : { default: inputs.value },
    }),
    // Editor documents declare one multi-source `items` input, aggregated in compiled edge-id
    // order; legacy `a`/`b` merges pass the first active value through.
    "control.merge": ({ inputs }) => {
      if (hasOwn(inputs, "items")) {
        return { outputs: { out: inputs.items } };
      }
      return { outputs: { out: hasOwn(inputs, "a") ? inputs.a : inputs.b } };
    },
    "json.string": ({ node }) => ({ outputs: { value: node.data?.value ?? "" } }),
    "json.number": ({ node }) => ({ outputs: { value: node.data?.value ?? 0 } }),
    "json.boolean": ({ node }) => ({ outputs: { value: node.data?.value ?? false } }),
    "json.null": () => ({ outputs: { value: null } }),
    "json.array": ({ node, inputs }) => ({ outputs: { value: orderedArrayInputs(node, inputs) } }),
    "human.task.approval": humanApproval,
    "human.task.input": humanInput,
  };
}

function responseRecord(response: unknown): Record<string, unknown> {
  if (typeof response !== "object" || response === null || Array.isArray(response)) {
    throw new TypeError("The human task response must be an object.");
  }
  return response as Record<string, unknown>;
}

/**
 * Approval step (workflow-editor `human.task.approval`, Engine TECH-001): suspends the run with the
 * compiled task, then routes the `request` value to the output named by the response decision.
 */
const humanApproval: WorkflowNodeExecutor = ({ node, inputs, resume }) => {
  if (!resume) {
    return {
      suspend: {
        taskId: node.id,
        request: { humanTask: node.humanTask ?? null, request: inputs.request ?? null },
      },
    };
  }
  const { decision } = responseRecord(resume.response);
  if (decision !== "approved" && decision !== "rejected") {
    throw new TypeError('The approval decision must be "approved" or "rejected".');
  }
  return { outputs: { [decision]: inputs.request } };
};

/** Typed human input step (`human.task.input`): suspends, then emits the response value. */
const humanInput: WorkflowNodeExecutor = ({ node, inputs, resume }) => {
  if (!resume) {
    return {
      suspend: {
        taskId: node.id,
        request: {
          humanTask: node.humanTask ?? null,
          ...(hasOwn(inputs, "context") ? { context: inputs.context } : {}),
        },
      },
    };
  }
  const response = responseRecord(resume.response);
  if (!hasOwn(response, "value")) {
    throw new TypeError("The human input response needs a value.");
  }
  return { outputs: { value: response.value } };
};
