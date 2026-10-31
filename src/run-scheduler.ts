import { executeIteration } from "./iteration-node.js";
import { buildInputs } from "./node-data.js";
import { executeNode } from "./node-execution.js";
import { executeReference } from "./reference-node.js";
import { type NodeOutcome, recordNodeFailure, type RunContext } from "./run-context.js";
import { fail } from "./run-errors.js";
import type { RunStreams } from "./run-streams.js";
import type {
  ExecutableWorkflowEdge,
  ExecutableWorkflowNode,
  WorkflowExecutorRegistration,
  WorkflowRunCheckpoint,
  WorkflowRunError,
} from "./types.js";

export type RunStop =
  | { kind: "failed"; error: WorkflowRunError }
  | { kind: "cancelled" }
  | { kind: "suspended"; waiting: WorkflowRunCheckpoint["waiting"] };

type SettledNode = { nodeId: string; outcome: NodeOutcome };

/** Decides which nodes of one graph run start, and when the run stops. */
export class RunScheduler {
  stop: RunStop | undefined;
  private readonly pending: string[];
  private readonly running = new Map<string, Promise<SettledNode>>();
  private readonly nodeById: ReadonlyMap<string, ExecutableWorkflowNode>;

  constructor(
    private readonly context: RunContext,
    private readonly streams: RunStreams,
    private readonly executors: ReadonlyMap<string, WorkflowExecutorRegistration>,
    private readonly incomingByNodeId: ReadonlyMap<string, ExecutableWorkflowEdge[]>,
    private readonly abortRun: (reason: Error) => void,
  ) {
    const { workflow, nodeResults } = context;
    this.nodeById = new Map(workflow.nodes.map((node) => [node.id, node] as const));
    this.pending = workflow.order.filter((nodeId) => !nodeResults[nodeId]);
  }

  /** Starts whatever can start; returns false once nothing is left in flight. */
  advance(): boolean {
    const { request, runSignal } = this.context;
    if (!this.stop && request.signal?.aborted) {
      this.stop = { kind: "cancelled" };
    }
    if (!this.stop) {
      this.launchReadyNodes();
      if (!this.stop && this.running.size === 0 && this.pending.length > 0) {
        this.stop = {
          kind: "failed",
          error: fail(
            "stream-deadlock",
            `Nodes ${this.pending.join(", ")} can never start: their stream consumers cannot run together.`,
          ),
        };
      }
    }
    if (this.stop && this.stop.kind !== "suspended" && !runSignal.aborted) {
      // A failed run starts no new work and stops what is still in flight.
      this.abortRun(new Error("Workflow run stopped."));
    }
    return !this.running.values().next().done;
  }

  /** Resolves with the first in-flight node to settle; call only after `advance` returned true. */
  nextSettled(): Promise<SettledNode> {
    return Promise.race(this.running.values());
  }

  settle({ nodeId, outcome }: SettledNode): void {
    this.running.delete(nodeId);
    if (this.stop) {
      return;
    }
    if (outcome.kind === "failed") {
      this.stop = { kind: "failed", error: outcome.error };
    } else if (outcome.kind === "cancelled") {
      this.stop = { kind: "cancelled" };
    } else if (outcome.kind === "suspended") {
      // Let in-flight nodes finish so their results join the checkpoint.
      this.stop = { kind: "suspended", waiting: outcome.checkpointWaiting };
    }
  }

  private isReady = (nodeId: string): boolean => {
    const { nodeResults } = this.context;
    return (this.incomingByNodeId.get(nodeId) ?? []).every(
      (edge) => nodeResults[edge.sourceNodeId],
    );
  };

  /** Prepares and starts one ready node; returns false when the run must stop. */
  private launchNode(nodeId: string): boolean {
    const { context, streams, pending, running } = this;
    const { request, nodeResults, emit, timestamp } = context;
    pending.splice(pending.indexOf(nodeId), 1);
    const node = this.nodeById.get(nodeId);
    if (!node) {
      throw new Error(`Validated workflow is missing node ${nodeId}.`);
    }
    const incomingEdges = this.incomingByNodeId.get(nodeId) ?? [];
    const built = buildInputs(node, incomingEdges, nodeResults, streams.resolveValue);
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
      streams.trackRelease(streams.release(nodeId));
      return true;
    }
    if (built.error) {
      recordNodeFailure(context, nodeId, 0, built.inputs, built.error);
      this.stop = { kind: "failed", error: built.error };
      return false;
    }
    const finish = (outcome: NodeOutcome) =>
      streams.release(nodeId).then(() => ({ nodeId, outcome }));
    if (node.kind === "control.iterate" && node.iteration) {
      running.set(
        nodeId,
        executeIteration(context, node, node.iteration, built.inputs).then(finish),
      );
      return true;
    }
    if (node.workflowRef) {
      running.set(
        nodeId,
        executeReference(context, node, node.workflowRef, built.inputs).then(finish),
      );
      return true;
    }
    const executor = this.executors.get(node.kind);
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
      this.stop = { kind: "failed", error };
      return false;
    }
    running.set(nodeId, executeNode(context, streams, node, built.inputs, executor).then(finish));
    return true;
  }

  /**
   * Consumers of one stream start together so a consumer waiting for a worker slot never
   * backpressures the producer into a deadlock.
   */
  private streamGroup(nodeId: string): string[] {
    const group = new Set<string>([nodeId]);
    for (const edge of this.incomingByNodeId.get(nodeId) ?? []) {
      if (!this.streams.has(edge)) {
        continue;
      }
      for (const sibling of this.streams.siblings(edge)) {
        if (this.pending.includes(sibling.targetNodeId)) {
          group.add(sibling.targetNodeId);
        }
      }
    }
    return this.context.workflow.order.filter((id) => group.has(id));
  }

  /** Launches ready nodes in compiled order, up to this run's worker limit (TECH-012). */
  private launchReadyNodes(): void {
    const { maxWorkers } = this.context;
    const { pending, running } = this;
    let progressed = true;
    while (progressed && !this.stop) {
      progressed = false;
      for (const nodeId of pending.slice()) {
        if (this.stop || running.size >= maxWorkers) {
          return;
        }
        if (!pending.includes(nodeId) || !this.isReady(nodeId)) {
          continue;
        }
        const group = this.streamGroup(nodeId);
        if (group.length > maxWorkers) {
          this.stop = {
            kind: "failed",
            error: fail(
              "stream-consumers-exceed-workers",
              `Nodes ${group.join(", ")} consume one stream together and need ${group.length} workers; maxWorkers is ${maxWorkers}.`,
              { nodeId },
            ),
          };
          return;
        }
        if (!group.every(this.isReady) || running.size + group.length > maxWorkers) {
          continue;
        }
        progressed = true;
        for (const member of group) {
          if (!this.launchNode(member)) {
            return;
          }
        }
      }
    }
  }
}
