const DEFAULT_MAX_CONCURRENCY = 4;

/**
 * A semaphore bounding how many `run()` callbacks execute at once. A single
 * shared instance (see {@link defaultConcurrencyLimiter}) enforces its cap
 * *globally* across concurrently invoked `copy()`/`move()` calls, not just
 * within one call - two folder copies started at the same time and sharing
 * a limiter will never together exceed its `maxConcurrency`.
 */
export class ConcurrencyLimiter {
  #maxConcurrency: number;
  #active = 0;
  #queue: (() => void)[] = [];

  public constructor(maxConcurrency: number) {
    ConcurrencyLimiter.#validate(maxConcurrency);
    this.#maxConcurrency = maxConcurrency;
  }

  public get maxConcurrency(): number {
    return this.#maxConcurrency;
  }

  public setMaxConcurrency(maxConcurrency: number): void {
    ConcurrencyLimiter.#validate(maxConcurrency);
    this.#maxConcurrency = maxConcurrency;
    this.#drain();
  }

  public async run<T>(fn: () => Promise<T>): Promise<T> {
    await this.#acquire();
    try {
      return await fn();
    } finally {
      this.#active -= 1;
      this.#drain();
    }
  }

  #acquire(): Promise<void> {
    if (this.#active < this.#maxConcurrency) {
      this.#active += 1;
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.#queue.push(() => {
        this.#active += 1;
        resolve();
      });
    });
  }

  #drain(): void {
    while (this.#active < this.#maxConcurrency && this.#queue.length > 0) {
      this.#queue.shift()?.();
    }
  }

  static #validate(maxConcurrency: number): void {
    if (!Number.isInteger(maxConcurrency) || maxConcurrency < 1) {
      throw new Error(`maxConcurrency must be a positive integer, got ${maxConcurrency}`);
    }
  }
}

/**
 * Process-wide default limiter, used by `copy()`/`move()` whenever
 * `TransferOptions.concurrencyLimiter` is omitted - so any two calls in the
 * same process share one cap unless a caller explicitly opts out with its
 * own instance (e.g. for test isolation).
 */
export const defaultConcurrencyLimiter = new ConcurrencyLimiter(DEFAULT_MAX_CONCURRENCY);

/** Minimal single-consumer async FIFO queue backing {@link mapAsyncIterableConcurrently}. */
class AsyncChannel<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #waiters: { resolve: (result: IteratorResult<T>) => void; reject: (error: unknown) => void }[] =
    [];
  #closed = false;
  #hasError = false;
  #error: unknown;

  push(item: T): void {
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter.resolve({ value: item, done: false });
    } else {
      this.#items.push(item);
    }
  }

  close(): void {
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      if (this.#hasError) {
        waiter.reject(this.#error);
      } else {
        waiter.resolve({ value: undefined as never, done: true });
      }
    }
  }

  fail(error: unknown): void {
    this.#hasError = true;
    this.#error = error;
    this.close();
  }

  async #next(): Promise<IteratorResult<T>> {
    if (this.#items.length > 0) {
      return { value: this.#items.shift() as T, done: false };
    }
    if (this.#closed) {
      if (this.#hasError) {
        throw this.#error;
      }
      return { value: undefined as never, done: true };
    }
    return new Promise((resolve, reject) => this.#waiters.push({ resolve, reject }));
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return { next: () => this.#next() };
  }
}

/**
 * Pulls items from `source` and runs `fn` on each, bounded by `limiter`:
 * exactly `limiter.maxConcurrency` worker loops each pull-then-process one
 * item at a time through `limiter.run`, so at most that many items are ever
 * pulled/in-flight at once (important for sources like a multipart reader
 * where pulling an item eagerly opens a file/network handle). Results are
 * yielded in completion order, not source order. If `fn` throws, no new
 * items are pulled but already-in-flight ones are allowed to finish before
 * the error is rethrown from the returned iterable.
 */
export function mapAsyncIterableConcurrently<T, R>(
  source: AsyncIterable<T>,
  fn: (item: T) => Promise<R>,
  limiter: ConcurrencyLimiter,
): AsyncIterable<R> {
  const channel = new AsyncChannel<R>();
  const iterator = source[Symbol.asyncIterator]();
  let stopped = false;
  let firstError: unknown;
  let errorRecorded = false;

  async function worker(): Promise<void> {
    for (;;) {
      if (stopped) return;
      const { done, value } = await iterator.next();
      if (done) return;
      try {
        const result = await limiter.run(() => fn(value));
        channel.push(result);
      } catch (error) {
        stopped = true;
        if (!errorRecorded) {
          errorRecorded = true;
          firstError = error;
        }
        return;
      }
    }
  }

  void (async () => {
    const workerCount = Math.max(1, limiter.maxConcurrency);
    await Promise.all(Array.from({ length: workerCount }, () => worker()));
    if (errorRecorded) {
      channel.fail(firstError);
    } else {
      channel.close();
    }
  })();

  return channel;
}
