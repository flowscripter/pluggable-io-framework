import { describe, expect, test } from "bun:test";
import { createAbortError, throwIfAborted, watchSignals } from "../../src/util/abort.ts";

describe("abort", () => {
  test("createAbortError is a DOMException named AbortError", () => {
    const error = createAbortError();
    expect(error).toBeInstanceOf(DOMException);
    expect(error.name).toBe("AbortError");
  });

  test("throwIfAborted only throws for an aborted signal", () => {
    expect(() => throwIfAborted(undefined)).not.toThrow();
    const controller = new AbortController();
    expect(() => throwIfAborted(controller.signal)).not.toThrow();
    controller.abort();
    expect(() => throwIfAborted(controller.signal)).toThrow("aborted");
  });

  test("watchSignals resolves with the signal that fires", async () => {
    const signal = new AbortController();
    const stop = new AbortController();
    const watch = watchSignals(signal.signal, stop.signal);
    stop.abort();
    expect(await watch.fired).toBe("stopped");
    watch.dispose();

    const aborted = new AbortController();
    aborted.abort();
    expect(await watchSignals(aborted.signal, undefined).fired).toBe("aborted");
  });

  test("dispose removes listeners so a later abort is ignored", async () => {
    const signal = new AbortController();
    const watch = watchSignals(signal.signal, undefined);
    watch.dispose();
    signal.abort();
    const outcome = await Promise.race([watch.fired, Promise.resolve("pending")]);
    expect(outcome).toBe("pending");
  });
});
