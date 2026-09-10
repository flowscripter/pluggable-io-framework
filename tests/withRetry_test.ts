import { describe, expect, test } from "bun:test";
import { PermanentIOError, TransientIOError } from "@flowscripter/pluggable-io-framework-api";
import { withRetry } from "../src/withRetry.ts";

describe("withRetry", () => {
  test("returns the result on first success without retrying", async () => {
    let calls = 0;
    const result = await withRetry(
      async () => {
        calls += 1;
        return "ok";
      },
      { maxRetries: 3 },
    );

    expect(result).toBe("ok");
    expect(calls).toBe(1);
  });

  test("retries a TransientIOError up to maxRetries then succeeds", async () => {
    let calls = 0;
    const result = await withRetry(
      async () => {
        calls += 1;
        if (calls < 3) throw new TransientIOError("timed out");
        return "ok";
      },
      { maxRetries: 3, backoffMs: () => 0 },
    );

    expect(result).toBe("ok");
    expect(calls).toBe(3);
  });

  test("gives up after maxRetries and surfaces the TransientIOError", async () => {
    let calls = 0;
    let thrown: unknown;
    try {
      await withRetry(
        async () => {
          calls += 1;
          throw new TransientIOError("always fails");
        },
        { maxRetries: 2, backoffMs: () => 0 },
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(TransientIOError);
    expect(calls).toBe(3); // initial attempt + 2 retries
  });

  test("does not retry a PermanentIOError", async () => {
    let calls = 0;
    let thrown: unknown;
    try {
      await withRetry(
        async () => {
          calls += 1;
          throw new PermanentIOError("not found");
        },
        { maxRetries: 3, backoffMs: () => 0 },
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(PermanentIOError);
    expect(calls).toBe(1);
  });

  test("does not retry a plain unclassified Error", async () => {
    let calls = 0;
    let thrown: unknown;
    try {
      await withRetry(
        async () => {
          calls += 1;
          throw new Error("boom");
        },
        { maxRetries: 3, backoffMs: () => 0 },
      );
    } catch (error) {
      thrown = error;
    }

    expect((thrown as Error).message).toBe("boom");
    expect(calls).toBe(1);
  });
});
