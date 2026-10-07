import { describe, expect, test } from "bun:test";
import {
  isRangeReadable,
  type TelemetryHooks,
  TransientIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { ConcurrencyLimiter } from "../../src/concurrency/ConcurrencyLimiter.ts";
import { multipartTransfer } from "../../src/transfer/multipartTransfer.ts";
import type { TransferOptions } from "../../src/transfer/TransferOptions.ts";
import {
  makeMemoryProvider,
  makeStore,
  type MemoryProviderOptions,
  readText,
} from "../fixtures/memoryProvider.ts";

const content = "hello world, this is a test payload";

async function run(
  sourceOptions: MemoryProviderOptions,
  options: TransferOptions = {},
  hooks: TelemetryHooks = {},
) {
  const source = makeMemoryProvider(makeStore({ a: content }), sourceOptions);
  const sinkStore = makeStore();
  const sink = makeMemoryProvider(sinkStore, { multipart: true });
  const readable = await source.getReadableStream("a");
  if (!isRangeReadable(readable) || !sink.getMultipartWriter) throw new Error("bad fixture");
  const result = await multipartTransfer({
    readable,
    sink,
    writer: sink.getMultipartWriter("b", 10),
    totalBytes: content.length,
    partSize: 10,
    limiter: new ConcurrencyLimiter(2),
    context: { operationId: "op", hooks, options, type: "copy" },
  });
  return { result, sinkStore };
}

describe("multipartTransfer", () => {
  test("assembles every part in order", async () => {
    const { result, sinkStore } = await run({ itemSize: 3 });
    expect(readText(sinkStore, "b")).toBe(content);
    expect(result).toEqual({ bytes: content.length, items: 14, stopped: false });
  });

  test("parts report their own operationId tagged with the transfer's operationId", async () => {
    const events: { operationId: string; parentOperationId?: string }[] = [];
    await run({}, {}, { onProgress: (event) => events.push(event) });
    const children = events.filter((event) => event.operationId !== "op");
    expect(children.length).toBe(4);
    expect(children.every((event) => event.parentOperationId === "op")).toBe(true);
    expect(events.some((event) => event.operationId === "op")).toBe(true);
  });

  test("re-reads only the failed part within the same upload", async () => {
    const failed = new Set<number>();
    const { sinkStore } = await run(
      {
        itemSize: 5,
        onRead: (_path, offset) => {
          if (offset === 15 && !failed.has(offset)) {
            failed.add(offset);
            throw new TransientIOError("part read failed");
          }
        },
      },
      { retry: { maxRetries: 2, backoffMs: () => 0 } },
    );
    expect(readText(sinkStore, "b")).toBe(content);
  });

  test("a part that keeps failing fails the transfer", async () => {
    await expect(
      run(
        {
          onRead: (_path, offset) => {
            if (offset === 20) throw new TransientIOError("part read failed");
          },
        },
        { retry: { maxRetries: 1, backoffMs: () => 0 } },
      ),
    ).rejects.toThrow("part read failed");
  });

  test("stop ends the transfer after the parts already started", async () => {
    const stop = new AbortController();
    stop.abort();
    const { result, sinkStore } = await run({}, { stop: stop.signal });
    expect(result.stopped).toBe(true);
    expect(readText(sinkStore, "b")).toBe("");
  });

  test("signal aborts the transfer", async () => {
    const signal = new AbortController();
    signal.abort();
    await expect(run({}, { signal: signal.signal })).rejects.toThrow("aborted");
  });

  test("signal during a part read aborts the transfer", async () => {
    const signal = new AbortController();
    await expect(
      run(
        {
          itemSize: 2,
          onRead: (_path, offset) => {
            if (offset === 2) signal.abort();
          },
        },
        { signal: signal.signal },
      ),
    ).rejects.toThrow("aborted");
  });
});
