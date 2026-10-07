import { describe, expect, test } from "bun:test";
import {
  type IOProvider,
  type Item,
  PayloadKind,
  type ResumeToken,
  type TelemetryHooks,
  TransientIOError,
} from "@flowscripter/pluggable-io-framework-api";
import type { RetryOptions } from "../../src/retry/RetryOptions.ts";
import { streamTransfer } from "../../src/transfer/streamTransfer.ts";
import type { TransferOptions } from "../../src/transfer/TransferOptions.ts";
import {
  makeMemoryProvider,
  makeRecordingSink,
  makeStore,
  type MemoryProviderOptions,
  readText,
} from "../fixtures/memoryProvider.ts";

const retry: RetryOptions = { maxRetries: 3, backoffMs: () => 0 };

function failOnce(offsets: number[]): (path: string, offset: number) => void {
  const pending = new Set(offsets);
  return (_path, offset) => {
    if (pending.delete(offset)) throw new TransientIOError(`failed at ${offset}`);
  };
}

async function run(
  source: IOProvider,
  sink: IOProvider,
  options: TransferOptions = {},
  hooks: TelemetryHooks = {},
) {
  return streamTransfer({
    source,
    sourceKey: "a",
    readable: await source.getReadableStream("a"),
    sink,
    destKey: "b",
    writable: await sink.getWritableStream("b"),
    totalBytes: (await source.getProperties("a")).size,
    context: { operationId: "op", hooks, options, type: "copy" },
  });
}

function setup(sourceOptions: MemoryProviderOptions = {}, sinkOptions: MemoryProviderOptions = {}) {
  const sourceStore = makeStore({ a: "abcdefghij" });
  const sinkStore = makeStore();
  return {
    sinkStore,
    source: makeMemoryProvider(sourceStore, { itemSize: 2, ...sourceOptions }),
    sink: makeMemoryProvider(sinkStore, sinkOptions),
  };
}

describe("streamTransfer signal and stop", () => {
  test("signal cancels the source, aborts the sink and rejects with AbortError", async () => {
    const controller = new AbortController();
    const { source, sink, sinkStore } = setup({
      onRead: (_path, offset) => {
        if (offset === 4) controller.abort();
      },
    });
    const error = await run(source, sink, { signal: controller.signal }).catch((e: unknown) => e);
    expect((error as DOMException).name).toBe("AbortError");
    expect(sinkStore.events).toContain("abort:b");
    expect(sinkStore.files.has("b")).toBe(false);
  });

  test("stop closes the sink normally and reports a truncated, stopped result", async () => {
    const controller = new AbortController();
    const { source, sink, sinkStore } = setup({
      onRead: (_path, offset) => {
        if (offset === 4) controller.abort();
      },
    });
    const result = await run(source, sink, { stop: controller.signal });
    expect(result.stopped).toBe(true);
    expect(sinkStore.events).toContain("close:b");
    expect(readText(sinkStore, "b").length).toBeLessThan(10);
  });
});

describe("streamTransfer retry and resume", () => {
  test("resumes a bounded transfer from the sink's committed offset", async () => {
    const resumes: (ResumeToken | undefined)[] = [];
    const { source, sink, sinkStore } = setup(
      { onRead: failOnce([6]) },
      { resumable: true, onOpenWrite: (_path, resume) => resumes.push(resume) },
    );
    const result = await run(source, sink, { retry });
    expect(readText(sinkStore, "b")).toBe("abcdefghij");
    expect(resumes.at(-1)).toEqual({ offset: 6 });
    expect(result.bytes).toBe(10);
  });

  test("resumes after a sink failure", async () => {
    let failed = false;
    const { source, sink, sinkStore } = setup(
      {},
      {
        resumable: true,
        onWrite: (_path, committed) => {
          if (committed === 4 && !failed) {
            failed = true;
            throw new TransientIOError("sink hiccup");
          }
        },
      },
    );
    await run(source, sink, { retry });
    expect(readText(sinkStore, "b")).toBe("abcdefghij");
  });

  test("restarts from scratch without a resume token", async () => {
    const { source, sink, sinkStore } = setup({ onRead: failOnce([6]) });
    const result = await run(source, sink, { retry });
    expect(readText(sinkStore, "b")).toBe("abcdefghij");
    expect(sinkStore.events).toContain("abort:b");
    expect(result.items).toBe(5);
  });

  test("restarts from scratch when the source is not RangeReadable", async () => {
    const { source, sink, sinkStore } = setup(
      { onRead: failOnce([6]), rangeReadable: false },
      { resumable: true },
    );
    await run(source, sink, { retry });
    expect(readText(sinkStore, "b")).toBe("abcdefghij");
  });

  test("fails immediately on a non-transient error and after maxRetries", async () => {
    const plain = setup({
      onRead: () => {
        throw new Error("broken");
      },
    });
    await expect(run(plain.source, plain.sink, { retry })).rejects.toThrow("broken");

    const always = setup({
      onRead: () => {
        throw new TransientIOError("always");
      },
    });
    await expect(
      run(always.source, always.sink, { retry: { maxRetries: 1, backoffMs: () => 0 } }),
    ).rejects.toThrow("always");
  });

  test("counts a failure to reopen as another failure", async () => {
    let opens = 0;
    const { source, sink, sinkStore } = setup({
      onRead: failOnce([2]),
      onOpenRead: () => {
        opens += 1;
        if (opens === 2) throw new TransientIOError("reopen failed");
      },
    });
    await run(source, sink, { retry });
    expect(readText(sinkStore, "b")).toBe("abcdefghij");
    expect(opens).toBe(3);
  });
});

describe("streamTransfer unbounded sources", () => {
  test("reconnects, flags the first new item as a discontinuity and reports the gap", async () => {
    const gaps: { atBytes: number; attempt: number }[] = [];
    const { source } = setup({ bounded: false, onRead: failOnce([4, 6]) });
    const sink = makeRecordingSink(PayloadKind.Js);
    const result = await run(
      source,
      sink,
      { retry: { ...retry, maxRetries: 1 } },
      {
        onGap: (_operationId, event) => gaps.push(event),
      },
    );
    expect(gaps).toEqual([
      { atBytes: 4, attempt: 1 },
      { atBytes: 10, attempt: 1 },
    ]);
    const flagged = sink.written.map((item) => item.attributes?.discontinuity === true);
    expect(flagged).toEqual([false, false, true, false, false, true, false, false, false, false]);
    expect(result.stopped).toBe(false);
  });

  test("onGap fail turns a source failure into a transfer failure", async () => {
    const { source } = setup({ bounded: false, onRead: failOnce([4]) });
    await expect(
      run(source, makeRecordingSink(PayloadKind.Js), { retry: { ...retry, onGap: "fail" } }),
    ).rejects.toThrow("failed at 4");
  });

  test("progress has no total for an unbounded source", async () => {
    const totals: (number | undefined)[] = [];
    const { source, sink } = setup({ bounded: false });
    await run(source, sink, {}, { onProgress: (event) => totals.push(event.totalBytes) });
    expect(totals.every((total) => total === undefined)).toBe(true);
  });

  test("a sink failure reopens the sink with its token and keeps the source", async () => {
    let failed = false;
    let sourceOpens = 0;
    const { source, sink, sinkStore } = setup(
      {
        bounded: false,
        onOpenRead: () => {
          sourceOpens += 1;
        },
      },
      {
        resumable: true,
        onWrite: (_path, committed) => {
          if (committed === 4 && !failed) {
            failed = true;
            throw new TransientIOError("sink hiccup");
          }
        },
      },
    );
    await run(source, sink, { retry });
    expect(sourceOpens).toBe(1);
    expect(readText(sinkStore, "b")).toBe("abcdghij");
  });

  test("a sink failure without a resume token fails", async () => {
    const { source, sink } = setup(
      { bounded: false },
      {
        onWrite: () => {
          throw new TransientIOError("sink down");
        },
      },
    );
    await expect(run(source, sink, { retry })).rejects.toThrow("sink down");
  });
});

describe("streamTransfer payload conversion", () => {
  test("applies the converter to each item", async () => {
    const { source } = setup();
    const sink = makeRecordingSink(PayloadKind.Js);
    await run(source, sink, {
      converter: {
        from: { kind: PayloadKind.Js },
        to: { kind: PayloadKind.Js },
        cost: 0,
        convert: (item: Item) => ({ ...item, attributes: { converted: true } }),
      },
    });
    expect(sink.written.every((item) => item.attributes?.converted === true)).toBe(true);
  });
});
