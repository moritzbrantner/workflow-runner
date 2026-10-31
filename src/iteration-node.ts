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
import type {
  ExecutableWorkflow,
  ExecutableWorkflowIteration,
  ExecutableWorkflowIterationBoundary,
  ExecutableWorkflowNode,
  WorkflowRunError,
} from "./types.js";

/** Runs a compiled-v2 bounded iteration (TECH-002/006/007); the body is a nested graph run. */
export async function executeIteration(
  context: RunContext,
  node: ExecutableWorkflowNode,
  iteration: ExecutableWorkflowIteration,
  inputs: Record<string, unknown>,
): Promise<NodeOutcome> {
  const { request, workflow, shared, runSignal, nodeResults, emit, timestamp } = context;
  const nodeId = node.id;
  const finish = (error: WorkflowRunError) => failNestedNode(context, nodeId, inputs, error);
  emit({
    type: "node.started",
    runId: request.runId,
    nodeId,
    attempt: 1,
    timestamp: timestamp(),
  });
  let state = inputs.state;
  /** The last completed iteration's measurements (TECH-004); never a quality metric. */
  const measurements: Record<string, unknown> = {};
  let status: "converged" | "limit-exhausted" = "limit-exhausted";
  let iterations = 0;
  const seedNodeId = "@iteration-input";
  const body = iteration.body;
  const boundaryIds = [...new Set(body.inputs.map((input) => input.id))];
  const bodyWorkflow: ExecutableWorkflow = {
    format: workflow.format,
    // Version 2 so inner iterations run as ordinary iteration nodes (#43); version 3 with the
    // root closure so body nodes can call Workflow References (#45).
    version: workflow.version === 3 ? 3 : 2,
    ...(workflow.version === 3 ? { workflows: shared.workflows ?? workflow.workflows ?? {} } : {}),
    nodes: [
      {
        id: seedNodeId,
        kind: seedNodeId,
        outputs: boundaryIds.map((id) => ({ id, type: { kind: "any" } })),
      },
      ...body.nodes,
    ],
    edges: [
      ...body.inputs.map((input, index) => ({
        id: `@iteration-input-${index}`,
        sourceNodeId: seedNodeId,
        sourcePortId: input.id,
        targetNodeId: input.nodeId,
        targetPortId: input.portId,
      })),
      ...body.edges,
    ],
    order: [seedNodeId, ...body.order],
  };

  while (iterations < iteration.maxIterations) {
    if (runSignal.aborted) {
      return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
    }
    const boundaryValues: Record<string, unknown> = { ...inputs, state };
    const bodyResult = await context.runNested(nestedRunRequest(context, bodyWorkflow), {
      budget: shared.budget,
      ...(shared.workflows ? { workflows: shared.workflows } : {}),
      ...(shared.callStack ? { callStack: shared.callStack } : {}),
      persistence: {
        workflowDigest: shared.persistence?.workflowDigest ?? digestExecutableWorkflow(workflow),
        inputDigest: shared.persistence?.inputDigest ?? runInputDigest(request),
        iteration: { nodeId, iteration: iterations + 1 },
      },
      seed: {
        [seedNodeId]: {
          status: "succeeded",
          attempts: 0,
          inputs: {},
          outputs: Object.fromEntries(boundaryIds.map((id) => [id, boundaryValues[id]])),
        },
      },
    });
    if (bodyResult.status === "cancelled") {
      return finish(fail("node-cancelled", `Node ${nodeId} was cancelled.`, { nodeId }));
    }
    if (bodyResult.status === "suspended") {
      return finish(
        fail(
          "node-failed",
          `Node ${nodeId}: suspension inside an iteration body is not supported.`,
          {
            nodeId,
          },
        ),
      );
    }
    if (bodyResult.status === "failed") {
      const code =
        bodyResult.error.code === "budget-exhausted" ||
        bodyResult.error.code === "reference-depth-exhausted"
          ? bodyResult.error.code
          : "node-failed";
      return finish(
        fail(
          code,
          `Node ${nodeId} iteration ${iterations + 1} failed: ${bodyResult.error.message}`,
          {
            nodeId,
            attempt: 1,
            cause: bodyResult.error,
          },
        ),
      );
    }
    const read = (boundary: ExecutableWorkflowIterationBoundary | undefined) => {
      const result = boundary ? bodyResult.nodeResults[boundary.nodeId] : undefined;
      return boundary && result?.status === "succeeded" && hasOwn(result.outputs, boundary.portId)
        ? { value: result.outputs[boundary.portId] }
        : undefined;
    };
    const next = read(body.outputs.next);
    if (!next) {
      return finish(
        fail(
          "iteration-no-result",
          `Node ${nodeId} iteration ${iterations + 1} produced no next state.`,
          { nodeId },
        ),
      );
    }
    iterations += 1;
    state = next.value;
    for (const measurement of body.outputs.measurements ?? []) {
      const measured = read(measurement);
      if (measured) {
        measurements[measurement.id] = measured.value;
      } else {
        delete measurements[measurement.id];
      }
    }
    const converged =
      iteration.stop.kind === "body-output" && read(body.outputs.converged)?.value === true;
    emit({
      type: "node.iteration",
      runId: request.runId,
      nodeId,
      iteration: iterations,
      converged,
      timestamp: timestamp(),
    });
    if (converged) {
      status = "converged";
      break;
    }
  }

  const outputs = {
    [iteration.result.outputPortId]: {
      status,
      value: state,
      iterations,
      maxIterations: iteration.maxIterations,
      ...(iteration.body.outputs.measurements ? { measurements } : {}),
    },
  };
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
    outputs,
  });
  return { kind: "succeeded" };
}
