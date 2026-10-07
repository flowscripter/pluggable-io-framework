import { describe, expect, test } from "bun:test";
import { backoff, DEFAULT_RETRY, defaultBackoffMs } from "../../src/retry/RetryOptions.ts";

describe("RetryOptions", () => {
  test("defaults to three retries", () => {
    expect(DEFAULT_RETRY.maxRetries).toBe(3);
  });

  test("default backoff doubles and caps at 5s", () => {
    expect(defaultBackoffMs(1)).toBe(400);
    expect(defaultBackoffMs(2)).toBe(800);
    expect(defaultBackoffMs(10)).toBe(5000);
  });

  test("backoff waits for the configured delay", async () => {
    const attempts: number[] = [];
    await backoff({ maxRetries: 1, backoffMs: (attempt) => (attempts.push(attempt), 0) }, 2);
    expect(attempts).toEqual([2]);
  });
});
