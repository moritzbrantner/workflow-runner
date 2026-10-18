import { type ExecutableWorkflow, workflowRunCheckpointFormat } from "./types.js";

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.keys(record)
        .filter((key) => record[key] !== undefined)
        .sort()
        .map((key) => [key, canonicalize(record[key])]),
    );
  }
  return value;
}

/** `sha256:<hex>` of the canonical JSON of a compiled workflow; pure and runtime-independent. */
export function digestExecutableWorkflow(workflow: ExecutableWorkflow): string {
  return digestJsonValue(workflow);
}

/** `sha256:<hex>` of the canonical JSON of a JSON value. */
export function digestJsonValue(value: unknown): string {
  return `sha256:${sha256Hex(JSON.stringify(canonicalize(value)))}`;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Returns a reason when a checkpoint cannot resume `workflow` as run `runId`. */
export function findCheckpointProblem(
  checkpoint: unknown,
  workflow: ExecutableWorkflow,
  runId: string,
): string | undefined {
  if (!isRecord(checkpoint)) {
    return "Checkpoint must be an object.";
  }
  if (checkpoint.format !== workflowRunCheckpointFormat || checkpoint.version !== 1) {
    return `Checkpoint must use ${workflowRunCheckpointFormat} version 1.`;
  }
  if (checkpoint.runId !== runId) {
    return `Checkpoint belongs to run ${String(checkpoint.runId)}, not ${runId}.`;
  }
  if (checkpoint.workflowDigest !== digestExecutableWorkflow(workflow)) {
    return "Checkpoint was captured for a different compiled workflow.";
  }
  const waiting = checkpoint.waiting;
  const order = workflow.order;
  if (
    !isRecord(waiting) ||
    typeof waiting.nodeId !== "string" ||
    typeof waiting.taskId !== "string" ||
    typeof waiting.attempt !== "number" ||
    !order.includes(waiting.nodeId)
  ) {
    return "Checkpoint waiting node is missing or unknown.";
  }
  if (!isRecord(checkpoint.completed)) {
    return "Checkpoint completed results must be an object.";
  }
  const waitingIndex = order.indexOf(waiting.nodeId);
  for (const [nodeId, result] of Object.entries(checkpoint.completed)) {
    const index = order.indexOf(nodeId);
    if (index === -1 || index === waitingIndex) {
      return `Checkpoint completed node ${nodeId} is unknown or is the waiting node.`;
    }
    if (
      !isRecord(result) ||
      (result.status !== "succeeded" && result.status !== "skipped") ||
      !isRecord(result.outputs) ||
      !isRecord(result.inputs)
    ) {
      return `Checkpoint result for node ${nodeId} is malformed.`;
    }
  }
  return undefined;
}

const sha256K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

function sha256Hex(text: string): string {
  const message = new TextEncoder().encode(text);
  const bitLength = message.length * 8;
  const padded = new Uint8Array(Math.ceil((message.length + 9) / 64) * 64);
  padded.set(message);
  padded[message.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(padded.length - 8, Math.floor(bitLength / 0x100000000));
  view.setUint32(padded.length - 4, bitLength >>> 0);

  const hash = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  ];
  const w = new Uint32Array(64);
  const rotr = (value: number, bits: number) => (value >>> bits) | (value << (32 - bits));

  for (let offset = 0; offset < padded.length; offset += 64) {
    for (let i = 0; i < 16; i += 1) {
      w[i] = view.getUint32(offset + i * 4);
    }
    for (let i = 16; i < 64; i += 1) {
      const w15 = w[i - 15] ?? 0;
      const w2 = w[i - 2] ?? 0;
      const s0 = rotr(w15, 7) ^ rotr(w15, 18) ^ (w15 >>> 3);
      const s1 = rotr(w2, 17) ^ rotr(w2, 19) ^ (w2 >>> 10);
      w[i] = ((w[i - 16] ?? 0) + s0 + (w[i - 7] ?? 0) + s1) >>> 0;
    }

    const state = [...hash];
    for (let i = 0; i < 64; i += 1) {
      const [a = 0, b = 0, c = 0, d = 0, e = 0, f = 0, g = 0, h = 0] = state;
      const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const choice = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + choice + (sha256K[i] ?? 0) + (w[i] ?? 0)) >>> 0;
      const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + majority) >>> 0;
      state.splice(0, 8, (temp1 + temp2) >>> 0, a, b, c, (d + temp1) >>> 0, e, f, g);
    }
    for (let i = 0; i < 8; i += 1) {
      hash[i] = ((hash[i] ?? 0) + (state[i] ?? 0)) >>> 0;
    }
  }

  return hash.map((word) => word.toString(16).padStart(8, "0")).join("");
}
