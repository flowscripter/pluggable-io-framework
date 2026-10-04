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
 * own instance.
 */
export const defaultConcurrencyLimiter = new ConcurrencyLimiter(DEFAULT_MAX_CONCURRENCY);
