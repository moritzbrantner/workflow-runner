import { hasOwn } from "./node-data.js";
import { fail } from "./run-errors.js";
import { isAsyncIterable, StreamBroadcast } from "./stream.js";
import type {
  ExecutableWorkflow,
  ExecutableWorkflowEdge,
  ExecutableWorkflowNode,
  WorkflowRunError,
} from "./types.js";

const streamKey = (nodeId: string, portId: string) => `${nodeId}\u0000${portId}`;

/** Streamed outputs of one run, distributed to their consumers through bounded broadcasts (#12). */
export class RunStreams {
  private readonly broadcasts = new Map<string, StreamBroadcast>();
  private readonly outgoingByPort = new Map<string, ExecutableWorkflowEdge[]>();
  private readonly pendingReleases: Promise<void>[] = [];

  constructor(
    workflow: ExecutableWorkflow,
    private readonly incomingByNodeId: ReadonlyMap<string, ExecutableWorkflowEdge[]>,
    private readonly capacity: number,
    runSignal: AbortSignal,
  ) {
    for (const edge of workflow.edges) {
      const key = streamKey(edge.sourceNodeId, edge.sourcePortId);
      const outgoing = this.outgoingByPort.get(key) ?? [];
      outgoing.push(edge);
      this.outgoingByPort.set(key, outgoing);
    }
    // Stopping the run (cancellation or failure) ends every stream it owns.
    runSignal.addEventListener("abort", () => this.releaseEverything(), { once: true });
  }

  open(
    node: ExecutableWorkflowNode,
    outputs: Record<string, unknown>,
  ): WorkflowRunError | undefined {
    for (const port of node.outputs ?? []) {
      if (port.payload?.mode !== "streamed" || !hasOwn(outputs, port.id)) {
        continue;
      }
      const value = outputs[port.id];
      if (!isAsyncIterable(value)) {
        return fail(
          "invalid-stream-output",
          `Node ${node.id} output ${port.id} is streamed but the executor did not return an async iterable.`,
          { nodeId: node.id },
        );
      }
      const edges = this.outgoingByPort.get(streamKey(node.id, port.id)) ?? [];
      if (edges.length > 0) {
        this.broadcasts.set(
          streamKey(node.id, port.id),
          new StreamBroadcast(
            value,
            edges.map((edge) => edge.id),
            this.capacity,
          ),
        );
      }
    }
    return undefined;
  }

  has(edge: ExecutableWorkflowEdge): boolean {
    return this.streamFor(edge) !== undefined;
  }

  /** The edges leaving the same source port as `edge`, including `edge`. */
  siblings(edge: ExecutableWorkflowEdge): readonly ExecutableWorkflowEdge[] {
    return this.outgoingByPort.get(streamKey(edge.sourceNodeId, edge.sourcePortId)) ?? [];
  }

  resolveValue = (edge: ExecutableWorkflowEdge, value: unknown): unknown =>
    this.streamFor(edge)?.subscribe(edge.id) ?? value;

  /** A finished, failed or skipped consumer no longer holds back its streams (TECH-010). */
  async release(nodeId: string): Promise<void> {
    for (const edge of this.incomingByNodeId.get(nodeId) ?? []) {
      await this.streamFor(edge)?.release(edge.id);
    }
  }

  /** Tracks a release that must settle before the run reports its result. */
  trackRelease(release: Promise<void>): void {
    this.pendingReleases.push(release);
  }

  /** Closes any upstream whose consumers never ran, then awaits every outstanding release. */
  closeAll(): Promise<PromiseSettledResult<void>[]> {
    this.releaseEverything();
    return Promise.allSettled(this.pendingReleases);
  }

  private streamFor(edge: ExecutableWorkflowEdge): StreamBroadcast | undefined {
    return this.broadcasts.get(streamKey(edge.sourceNodeId, edge.sourcePortId));
  }

  private releaseEverything(): void {
    for (const broadcast of this.broadcasts.values()) {
      this.pendingReleases.push(broadcast.releaseAll());
    }
  }
}
