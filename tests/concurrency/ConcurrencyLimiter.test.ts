import { describe, expect, test } from "bun:test";
import { ConcurrencyLimiter } from "../../src/concurrency/ConcurrencyLimiter.ts";

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
