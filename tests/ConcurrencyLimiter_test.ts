import { describe, expect, test } from "bun:test";
import { ConcurrencyLimiter, mapAsyncIterableConcurrently } from "../src/ConcurrencyLimiter.ts";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe("ConcurrencyLimiter", () => {
  test("run() never exceeds maxConcurrency active callbacks", async () => {
    const limiter = new ConcurrencyLimiter(2);
    let active = 0;
    let maxActive = 0;

    const tasks = Array.from({ length: 6 }, () =>
      limiter.run(async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await delay(10);
        active -= 1;
      }),
    );
    await Promise.all(tasks);

    expect(maxActive).toBeLessThanOrEqual(2);
  });

  test("a single limiter instance bounds concurrency across independently-started run() batches", async () => {
    const limiter = new ConcurrencyLimiter(2);
    let active = 0;
    let maxActive = 0;

    async function batch(): Promise<void> {
      await Promise.all(
        Array.from({ length: 4 }, () =>
          limiter.run(async () => {
            active += 1;
            maxActive = Math.max(maxActive, active);
            await delay(10);
            active -= 1;
          }),
        ),
      );
    }

    await Promise.all([batch(), batch()]);

    expect(maxActive).toBeLessThanOrEqual(2);
  });

  test("setMaxConcurrency raises the cap for already-queued work", async () => {
    const limiter = new ConcurrencyLimiter(1);
    let active = 0;
    let maxActive = 0;

    const tasks = Array.from({ length: 4 }, () =>
      limiter.run(async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await delay(10);
        active -= 1;
      }),
    );
    limiter.setMaxConcurrency(4);
    await Promise.all(tasks);

    expect(maxActive).toBeGreaterThan(1);
  });

  test("constructor rejects a non-positive-integer maxConcurrency", () => {
    expect(() => new ConcurrencyLimiter(0)).toThrow();
    expect(() => new ConcurrencyLimiter(-1)).toThrow();
    expect(() => new ConcurrencyLimiter(1.5)).toThrow();
  });
});

async function* asyncRange(n: number): AsyncGenerator<number> {
  for (let i = 0; i < n; i += 1) {
    yield i;
  }
}

describe("mapAsyncIterableConcurrently", () => {
  test("yields a mapped result for every source item", async () => {
    const limiter = new ConcurrencyLimiter(3);
    const results: number[] = [];
    for await (const value of mapAsyncIterableConcurrently(
      asyncRange(10),
      (n) => Promise.resolve(n * 2),
      limiter,
    )) {
      results.push(value);
    }
    expect(results.sort((a, b) => a - b)).toEqual([0, 2, 4, 6, 8, 10, 12, 14, 16, 18]);
  });

  test("never has more than maxConcurrency items in flight", async () => {
    const limiter = new ConcurrencyLimiter(2);
    let active = 0;
    let maxActive = 0;

    const results: number[] = [];
    for await (const value of mapAsyncIterableConcurrently(
      asyncRange(8),
      async (n) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await delay(5);
        active -= 1;
        return n;
      },
      limiter,
    )) {
      results.push(value);
    }

    expect(results.length).toBe(8);
    expect(maxActive).toBeLessThanOrEqual(2);
  });

  test("propagates the first error and stops starting new work", async () => {
    const limiter = new ConcurrencyLimiter(2);
    const started: number[] = [];

    let thrown: unknown;
    try {
      for await (const _value of mapAsyncIterableConcurrently(
        asyncRange(20),
        async (n) => {
          started.push(n);
          await delay(5);
          if (n === 2) throw new Error(`boom on ${n}`);
          return n;
        },
        limiter,
      )) {
        // draining
      }
    } catch (error) {
      thrown = error;
    }

    expect((thrown as Error).message).toBe("boom on 2");
    // Only a handful of items should have started (limiter cap + in-flight at time of failure), not all 20.
    expect(started.length).toBeLessThan(20);
  });
});
