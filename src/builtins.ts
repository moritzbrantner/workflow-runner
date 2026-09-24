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
    "json.array": ({ node, inputs }) => ({ outputs: { value: orderedArrayInputs(node, inputs) } }),
  };
}
