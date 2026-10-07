import { describe, expect, test } from "bun:test";
import { ConcurrencyLimiter } from "../../src/concurrency/ConcurrencyLimiter.ts";
import { mapAsyncIterableConcurrently } from "../../src/concurrency/mapAsyncIterableConcurrently.ts";

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

  test("propagates an error thrown by the source iterable", async () => {
    async function* failing(): AsyncGenerator<number> {
      yield 1;
      throw new Error("source failed");
    }
    const results: number[] = [];
    const run = async () => {
      for await (const result of mapAsyncIterableConcurrently(
        failing(),
        async (n) => n,
        new ConcurrencyLimiter(1),
      )) {
        results.push(result);
      }
    };
    await expect(run()).rejects.toThrow("source failed");
  });
});
