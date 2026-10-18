import { test } from "bun:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createWorkflowRunner } from "./index.js";
import type { ExecutableWorkflow, WorkflowNodeExecutor } from "./types.js";

/** Copied unchanged from workflow-editor fixtures/compiled-v1-binary-ports.json. */
const fixture = JSON.parse(
  readFileSync(new URL("../fixtures/compiled-v1-binary-ports.json", import.meta.url), "utf8"),
) as ExecutableWorkflow;

/** A synthetic WAV byte sequence: RIFF header plus a deterministic sample ramp. */
const wav = new Uint8Array([
  ...new TextEncoder().encode("RIFF$\u0000\u0000\u0000WAVEfmt "),
  ...Array.from({ length: 28 }, (_, index) => (index * 37) % 256),
]);
const chunkSize = 4;
const tick = () => new Promise((resolve) => setImmediate(resolve));

async function collect(stream: unknown, onChunk?: () => Promise<void>): Promise<Uint8Array> {
  const parts: number[] = [];
  for await (const chunk of stream as AsyncIterable<Uint8Array>) {
    parts.push(...chunk);
    await onChunk?.();
  }
  return new Uint8Array(parts);
}

function audioRunner(
  options: {
    capacity?: number;
    previewStopsAfter?: number;
    onPreviewChunk?: (consumed: number) => void;
  } = {},
) {
  const evidence = {
    sourceRuns: 0,
    produced: 0,
    producerDone: false,
    producerClosed: false,
    previewConsumed: 0,
    maxLag: 0,
    consumedBeforeProducerDone: false,
    buffered: [] as Uint8Array[],
    preview: undefined as Uint8Array | undefined,
    transcribed: undefined as Uint8Array | undefined,
  };
  async function* chunks() {
    try {
      for (let offset = 0; offset < wav.length; offset += chunkSize) {
        evidence.produced += 1;
        evidence.maxLag = Math.max(evidence.maxLag, evidence.produced - evidence.previewConsumed);
        yield wav.slice(offset, offset + chunkSize);
      }
      evidence.producerDone = true;
    } finally {
      evidence.producerClosed = true;
    }
  }
  const bufferedConsumer: WorkflowNodeExecutor = ({ inputs }) => {
    evidence.buffered.push(inputs.wav as Uint8Array);
    return { outputs: {} };
  };
  const runner = createWorkflowRunner({
    ...(options.capacity ? { streamQueueCapacity: options.capacity } : {}),
    executors: {
      "audio.source": () => {
        evidence.sourceRuns += 1;
        return { outputs: { wav, chunks: chunks() } };
      },
      "audio.archive": bufferedConsumer,
      "audio.waveform": bufferedConsumer,
      "audio.preview": async ({ inputs }) => {
        const parts: number[] = [];
        for await (const chunk of inputs.chunks as AsyncIterable<Uint8Array>) {
          parts.push(...chunk);
          evidence.previewConsumed += 1;
          if (!evidence.producerDone) {
            evidence.consumedBeforeProducerDone = true;
          }
          options.onPreviewChunk?.(evidence.previewConsumed);
          if (evidence.previewConsumed === options.previewStopsAfter) {
            break;
          }
          // The preview is deliberately slower than transcription.
          await tick();
          await tick();
        }
        evidence.preview = new Uint8Array(parts);
        return { outputs: {} };
      },
      "audio.transcribe": async ({ inputs }) => {
        evidence.transcribed = await collect(inputs.chunks);
        return { outputs: { text: `${evidence.transcribed.length} bytes` } };
      },
    },
  });
  return { runner, evidence };
}

test("delivers buffered and streamed WAV bytes to every consumer with one producer run", async () => {
  const { runner, evidence } = audioRunner({ capacity: 2 });
  const result = await runner.dispatch({ runId: "audio", workflow: fixture });

  assert.equal(result.status, "succeeded");
  assert.equal(evidence.sourceRuns, 1);
  assert.equal(evidence.buffered.length, 2);
  assert.ok(evidence.buffered.every((bytes) => bytes === wav));
  assert.deepEqual(evidence.preview, wav);
  assert.deepEqual(evidence.transcribed, wav);
  assert.ok(evidence.consumedBeforeProducerDone, "consumption overlaps production");
  // Capacity 2 per consumer plus the chunk being handed over: the producer never runs ahead.
  assert.ok(evidence.maxLag <= 3, `producer ran ${evidence.maxLag} chunks ahead`);
  assert.ok(evidence.producerClosed);
  // Lifecycle events stay JSON-safe: no raw bytes or live streams.
  const succeeded = result.events.find(
    (event) => event.type === "node.succeeded" && event.nodeId === "wav-source",
  );
  assert.deepEqual(succeeded?.type === "node.succeeded" && succeeded.outputs, {
    wav: { payload: "binary", byteLength: wav.length },
    chunks: { payload: "stream" },
  });
  assert.doesNotThrow(() => JSON.parse(JSON.stringify(result.events)));
});

test("a consumer that stops early is released while the other consumer keeps receiving", async () => {
  const { runner, evidence } = audioRunner({ capacity: 1, previewStopsAfter: 2 });
  const result = await runner.dispatch({ runId: "early-stop", workflow: fixture });

  assert.equal(result.status, "succeeded");
  assert.deepEqual(evidence.preview, wav.slice(0, 2 * chunkSize));
  assert.deepEqual(evidence.transcribed, wav);
});

test("cancellation during an active stream closes the producer", async () => {
  const controller = new AbortController();
  const { runner, evidence } = audioRunner({
    capacity: 1,
    onPreviewChunk: (consumed) => {
      if (consumed === 2) {
        controller.abort();
      }
    },
  });
  const result = await runner.dispatch({
    runId: "cancel",
    workflow: fixture,
    signal: controller.signal,
  });

  assert.equal(result.status, "cancelled");
  assert.ok(evidence.producerClosed);
  assert.ok(!evidence.producerDone);
});

test("rejects buffered-to-streamed connections before any executor runs", async () => {
  const { runner, evidence } = audioRunner();
  const workflow = structuredClone(fixture);
  const edge = workflow.edges.find((candidate) => candidate.id === "wav-archive")!;
  edge.sourcePortId = "chunks";
  const result = await runner.dispatch({ runId: "mismatch", workflow });

  assert.equal(result.status, "failed");
  if (result.status === "failed") {
    assert.equal(result.error.code, "invalid-workflow");
    assert.ok(
      result.error.diagnostics?.some((diagnostic) => diagnostic.code === "payload-mode-mismatch"),
    );
  }
  assert.equal(evidence.sourceRuns, 0);
});

test("fails clearly when stream consumers cannot run together or no stream is returned", async () => {
  const { runner } = audioRunner();
  const tooFew = await runner.dispatch({ runId: "workers", workflow: fixture, maxWorkers: 1 });
  assert.equal(tooFew.status === "failed" && tooFew.error.code, "stream-consumers-exceed-workers");

  runner.registerExecutor("audio.source", () => ({ outputs: { wav, chunks: wav } }));
  const notStream = await runner.dispatch({ runId: "not-stream", workflow: fixture });
  assert.equal(notStream.status === "failed" && notStream.error.code, "invalid-stream-output");
});
