/**
 * Distributes one streamed output to every active consumer (TECH-004/TECH-010).
 *
 * Each consumer has a bounded queue. The single upstream iterator is pulled only while every active
 * consumer has room, so the slowest active consumer backpressures the producer and no chunk is
 * dropped or buffered without bound. A consumer that finishes, fails or is cancelled is released;
 * the others continue. When no active consumer remains, the upstream iterator is closed.
 */
export class StreamBroadcast {
  readonly #source: AsyncIterable<unknown>;
  readonly #capacity: number;
  readonly #queues = new Map<string, ConsumerQueue>();
  #iterator: AsyncIterator<unknown> | undefined;
  #started = false;
  #finished = false;
  #failure: { error: unknown } | undefined;
  #wakePump: (() => void) | undefined;

  constructor(source: AsyncIterable<unknown>, consumerIds: readonly string[], capacity: number) {
    this.#source = source;
    this.#capacity = capacity;
    for (const id of consumerIds) {
      this.#queues.set(id, { chunks: [], released: false, wake: undefined });
    }
  }

  /** The iterable a consumer reads; each consumer id receives every chunk exactly once. */
  subscribe(consumerId: string): AsyncIterable<unknown> {
    const queue = this.#queues.get(consumerId);
    if (!queue) {
      throw new Error(`Stream consumer ${consumerId} is not registered.`);
    }
    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<unknown>> => {
          this.#start();
          while (true) {
            if (queue.released) {
              return { done: true, value: undefined };
            }
            if (queue.chunks.length > 0) {
              const value = queue.chunks.shift();
              this.#wakePump?.();
              return { done: false, value };
            }
            if (this.#failure) {
              throw this.#failure.error;
            }
            if (this.#finished) {
              return { done: true, value: undefined };
            }
            await new Promise<void>((resolve) => {
              queue.wake = resolve;
            });
          }
        },
        return: async (): Promise<IteratorResult<unknown>> => {
          await this.release(consumerId);
          return { done: true, value: undefined };
        },
      }),
    };
  }

  /** Releases a consumer's queue; closes the upstream when no active consumer remains. */
  async release(consumerId: string): Promise<void> {
    const queue = this.#queues.get(consumerId);
    if (!queue || queue.released) {
      return;
    }
    queue.released = true;
    queue.chunks.length = 0;
    queue.wake?.();
    this.#wakePump?.();
    if (this.#active().length === 0) {
      await this.#close();
    }
  }

  /** Releases every consumer and closes the upstream iterator. */
  async releaseAll(): Promise<void> {
    for (const id of this.#queues.keys()) {
      await this.release(id);
    }
    await this.#close();
  }

  #active(): ConsumerQueue[] {
    return [...this.#queues.values()].filter((queue) => !queue.released);
  }

  #start(): void {
    if (!this.#started) {
      this.#started = true;
      // The pump records its own failure for consumers; it never rejects.
      this.#pump().catch(() => undefined);
    }
  }

  async #close(): Promise<void> {
    if (!this.#finished) {
      this.#finished = true;
      this.#wakePump?.();
      await this.#iterator?.return?.();
    }
  }

  async #pump(): Promise<void> {
    this.#iterator = this.#source[Symbol.asyncIterator]();
    try {
      while (!this.#finished) {
        const active = this.#active();
        if (active.length === 0) {
          break;
        }
        if (active.some((queue) => queue.chunks.length >= this.#capacity)) {
          await new Promise<void>((resolve) => {
            this.#wakePump = resolve;
          });
          continue;
        }
        const step = await this.#iterator.next();
        if (step.done || this.#finished) {
          break;
        }
        for (const queue of this.#active()) {
          queue.chunks.push(step.value);
          queue.wake?.();
        }
      }
    } catch (error) {
      this.#failure = { error };
    } finally {
      this.#finished = true;
      for (const queue of this.#queues.values()) {
        queue.wake?.();
      }
    }
  }
}

type ConsumerQueue = {
  chunks: unknown[];
  released: boolean;
  wake: (() => void) | undefined;
};

export function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function"
  );
}

/** Replaces streams and raw bytes with JSON-safe summaries for lifecycle events. */
export function summarizeOutputs(outputs: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(outputs).map(([key, value]) => {
      if (isAsyncIterable(value)) {
        return [key, { payload: "stream" }];
      }
      if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
        return [key, { payload: "binary", byteLength: value.byteLength }];
      }
      return [key, value];
    }),
  );
}
