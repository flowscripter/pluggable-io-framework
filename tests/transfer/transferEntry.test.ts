import { describe, expect, test } from "bun:test";
import {
  type BufferLease,
  type IOProvider,
  type Item,
  type PayloadConverter,
  PayloadKind,
  PermanentIOError,
  TransientIOError,
} from "@flowscripter/pluggable-io-framework-api";
import {
  basePath,
  transferEntry,
  type TransferEntryInput,
} from "../../src/transfer/transferEntry.ts";
import {
  makeMemoryProvider,
  makeRecordingSink,
  makeStore,
  type MemoryProviderOptions,
  readText,
} from "../fixtures/memoryProvider.ts";

const noBackoff = { maxRetries: 3, backoffMs: () => 0 };

function setup(sourceOptions: MemoryProviderOptions = {}, sinkOptions: MemoryProviderOptions = {}) {
  const sourceStore = makeStore({ "a.txt": "hello world" });
  const sinkStore = makeStore();
  return {
    sourceStore,
    sinkStore,
    source: makeMemoryProvider(sourceStore, { id: "source", ...sourceOptions }),
    sink: makeMemoryProvider(sinkStore, { id: "sink", ...sinkOptions }),
  };
}

async function run(
  source: IOProvider,
  sink: IOProvider,
  overrides: Partial<TransferEntryInput> = {},
) {
  return transferEntry({
    source,
    sourceKey: "a.txt",
    sink,
    destKey: "b.txt",
    properties: await source.getProperties("a.txt"),
    options: {},
    type: "copy",
    deleteSource: false,
    ...overrides,
  });
}

describe("transferEntry", () => {
  test("streams between providers and reports progress with a stable operationId", async () => {
    const { source, sink, sinkStore } = setup({ itemSize: 4 });
    const events: { operationId: string; bytesProcessed: number }[] = [];
    const result = await run(source, sink, {
      options: { telemetry: { onProgress: (event) => events.push(event) } },
    });
    expect(readText(sinkStore, "b.txt")).toBe("hello world");
    expect(result).toEqual({ stopped: false, bytes: 11, items: 3, path: "js -> js, stream" });
    expect(new Set(events.map((e) => e.operationId)).size).toBe(1);
    expect(events.at(-1)?.bytesProcessed).toBe(11);
  });

  test("uses directCopy when eligible, unless directTransfer is false", async () => {
    const store = makeStore({ "a.txt": "hello" });
    const provider = makeMemoryProvider(store, { id: "same", direct: true });
    const direct = await run(provider, provider);
    expect(direct.path).toBe("js -> js, direct");
    expect(direct.bytes).toBe(5);
    expect(store.events).toContain("directCopy:a.txt->b.txt");

    const streamed = await run(provider, provider, {
      destKey: "c.txt",
      options: { directTransfer: false },
    });
    expect(streamed.path).toBe("js -> js, stream");
    expect(store.events).not.toContain("directCopy:a.txt->c.txt");
  });

  test("uses directMove for an eligible move", async () => {
    const store = makeStore({ "a.txt": "hello" });
    const provider = makeMemoryProvider(store, { id: "same", direct: true });
    await run(provider, provider, { type: "move", deleteSource: true });
    expect(store.events).toContain("directMove:a.txt->b.txt");
    expect(store.files.has("a.txt")).toBe(false);
  });

  test("uses multipart when the sink supports it, the source is RangeReadable and the size crosses the threshold", async () => {
    const { source, sink, sinkStore } = setup({}, { multipart: true });
    const result = await run(source, sink, { options: { multipartThreshold: 1 } });
    expect(result.path).toBe("js -> js, multipart");
    expect(readText(sinkStore, "b.txt")).toBe("hello world");

    const plain = setup({ rangeReadable: false }, { multipart: true });
    const streamed = await run(plain.source, plain.sink, { options: { multipartThreshold: 1 } });
    expect(streamed.path).toBe("js -> js, stream");
  });

  test("a failed multipart write resumes from the writer's token, or restarts without one", async () => {
    const partSizeConstraints = () => ({
      minPartSize: 1,
      maxPartSize: 4,
      maxParts: 100,
      defaultPartSize: 4,
    });
    const failOnce = () => {
      let failed = false;
      return (_path: string, offset: number) => {
        if (offset === 4 && !failed) {
          failed = true;
          throw new TransientIOError("part write failed");
        }
      };
    };
    const options = { multipartThreshold: 1, retry: noBackoff };

    const resumable = setup(
      {},
      { multipart: true, resumable: true, partSizeConstraints, onWrite: failOnce() },
    );
    const result = await run(resumable.source, resumable.sink, { options });
    expect(readText(resumable.sinkStore, "b.txt")).toBe("hello world");
    expect(result.bytes).toBe(11);
    expect(resumable.sinkStore.events).toEqual([
      "multipart:b.txt:start",
      "part:b.txt:0",
      "part:b.txt:1",
      "multipart:b.txt:resume",
      "part:b.txt:1",
      "part:b.txt:2",
    ]);

    const restarting = setup({}, { multipart: true, partSizeConstraints, onWrite: failOnce() });
    await run(restarting.source, restarting.sink, { options });
    expect(readText(restarting.sinkStore, "b.txt")).toBe("hello world");
    expect(restarting.sinkStore.events).toEqual([
      "multipart:b.txt:start",
      "part:b.txt:0",
      "part:b.txt:1",
      "multipart:b.txt:start",
      "part:b.txt:0",
      "part:b.txt:1",
      "part:b.txt:2",
    ]);
  });

  test("fails a kind mismatch without a converter, and applies a converter per item", async () => {
    const { source } = setup({ itemSize: 4 });
    const nativeSink = makeRecordingSink(PayloadKind.Native);
    await expect(run(source, nativeSink)).rejects.toThrow();

    let calls = 0;
    const converter: PayloadConverter = {
      from: { kind: PayloadKind.Js },
      to: { kind: PayloadKind.Native, domain: "host" },
      cost: 1,
      convert: (item) => {
        calls += 1;
        return item;
      },
    };
    const sink = makeRecordingSink(PayloadKind.Native);
    const result = await run(source, sink, { options: { converter } });
    expect(calls).toBe(3);
    expect(sink.written.length).toBe(3);
    expect(result.path).toBe("js -> native via js->native[host] (copy), stream");
  });

  test("reports the negotiated path when given", async () => {
    const { source, sink } = setup();
    const result = await run(source, sink, { options: { path: "file/js -> s3/js" } });
    expect(result.path).toBe("file/js -> s3/js, stream");
    expect(basePath(source, sink, {})).toBe("js -> js");
  });

  test("checks the source payload type against the sink's accepted types", async () => {
    const { source, sink, sinkStore } = setup({ payloadType: "urn:packets:1" });
    await expect(run(source, sink)).rejects.toThrow(
      'Source payload type "urn:packets:1" is not accepted by the sink (sink accepts: bytes)',
    );
    await run(source, sink, { options: { writePayloadTypes: ["bytes", "urn:packets:1"] } });
    expect(readText(sinkStore, "b.txt")).toBe("hello world");
  });

  test("move rejects an unbounded source before writing", async () => {
    const { source, sink, sourceStore, sinkStore } = setup({ bounded: false });
    const error = await run(source, sink, { type: "move", deleteSource: true }).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(PermanentIOError);
    expect((error as Error).message).toBe("cannot move a live source");
    expect(sourceStore.files.has("a.txt")).toBe(true);
    expect(sinkStore.files.has("b.txt")).toBe(false);
  });

  test("move requires delete on the source", async () => {
    const { source, sink } = setup();
    const { delete: _delete, ...noDelete } = source;
    await expect(run(noDelete, sink, { type: "move", deleteSource: true })).rejects.toThrow(
      "does not support delete",
    );
  });

  test("move deletes the source, but not when stopped", async () => {
    const moved = setup();
    await run(moved.source, moved.sink, { type: "move", deleteSource: true });
    expect(moved.sourceStore.files.has("a.txt")).toBe(false);

    const stop = new AbortController();
    const stopped = setup({
      itemSize: 2,
      onRead: (_path, offset) => {
        if (offset === 4) stop.abort();
      },
    });
    const result = await run(stopped.source, stopped.sink, {
      type: "move",
      deleteSource: true,
      options: { stop: stop.signal },
    });
    expect(result.stopped).toBe(true);
    expect(stopped.sourceStore.files.has("a.txt")).toBe(true);
  });

  test("an already stopped or aborted transfer does nothing", async () => {
    const { source, sink, sinkStore } = setup();
    const stop = new AbortController();
    stop.abort();
    expect(await run(source, sink, { options: { stop: stop.signal } })).toEqual({
      stopped: true,
      bytes: 0,
      items: 0,
      path: "js -> js",
    });
    const signal = new AbortController();
    signal.abort();
    await expect(run(source, sink, { options: { signal: signal.signal } })).rejects.toThrow(
      "aborted",
    );
    expect(sinkStore.files.size).toBe(0);
  });

  test("retries opening the source on TransientIOError, up to maxRetries", async () => {
    let attempts = 0;
    const flaky = setup({
      onOpenRead: () => {
        attempts += 1;
        if (attempts <= 2) throw new TransientIOError(`flaky attempt ${attempts}`);
      },
    });
    await run(flaky.source, flaky.sink, { options: { retry: noBackoff } });
    expect(readText(flaky.sinkStore, "b.txt")).toBe("hello world");

    attempts = -10;
    await expect(
      run(flaky.source, flaky.sink, { options: { retry: { maxRetries: 2, backoffMs: () => 0 } } }),
    ).rejects.toBeInstanceOf(TransientIOError);
  });

  test("does not retry a plain error", async () => {
    let attempts = 0;
    const { source, sink } = setup({
      onOpenRead: () => {
        attempts += 1;
        throw new Error("plain failure");
      },
    });
    await expect(run(source, sink, { options: { retry: noBackoff } })).rejects.toThrow(
      "plain failure",
    );
    expect(attempts).toBe(1);
  });
});

function makeNativeLeaseProviders(failFirstCommit: boolean, reopenWithoutLeases = false) {
  const written: number[] = [];
  let commits = 0;
  let sinkOpens = 0;
  const source: IOProvider = {
    kind: PayloadKind.Native,
    async [Symbol.asyncDispose]() {},
    async getProperties() {
      return { size: 4, lastModified: undefined, isContainer: false, properties: {} };
    },
    async getReadableStream() {
      let remaining = 2;
      return {
        kind: PayloadKind.Native,
        stream: new ReadableStream<Item>(),
        domains: ["host"],
        async readInto() {
          if (remaining === 0) return null;
          remaining -= 1;
          return 2;
        },
      };
    },
    async getWritableStream() {
      throw new Error("not a sink");
    },
  };
  const sink: IOProvider = {
    ...source,
    async getWritableStream() {
      sinkOpens += 1;
      const handle = { kind: PayloadKind.Native, stream: new WritableStream<Item>() };
      if (reopenWithoutLeases && sinkOpens > 1) return handle;
      return {
        ...handle,
        domain: "host",
        async acquire(): Promise<BufferLease> {
          return {
            ptr: 0,
            length: 8,
            domain: "host",
            async commit(length) {
              commits += 1;
              if (failFirstCommit && commits === 1) throw new TransientIOError("commit failed");
              written.push(length);
            },
            release() {},
          };
        },
      };
    },
  };
  return { source, sink, written };
}

describe("transferEntry lease path", () => {
  test("is used when the sink provides buffers in a domain the source can fill", async () => {
    const { source, sink, written } = makeNativeLeaseProviders(false);
    const result = await transferEntry({
      source,
      sourceKey: "a",
      sink,
      destKey: "b",
      properties: await source.getProperties("a"),
      options: { leaseDepth: 3 },
      type: "copy",
      deleteSource: false,
    });
    expect(result.path).toBe("native -> native, lease (depth 3)");
    expect(written).toEqual([2, 2]);
  });

  test("restarts with fresh handles after a TransientIOError", async () => {
    const { source, sink, written } = makeNativeLeaseProviders(true);
    const result = await transferEntry({
      source,
      sourceKey: "a",
      sink,
      destKey: "b",
      properties: await source.getProperties("a"),
      options: { retry: noBackoff },
      type: "copy",
      deleteSource: false,
    });
    expect(result.bytes).toBe(4);
    expect(written).toEqual([2, 2]);
  });

  test("fails if reopened handles no longer support leases", async () => {
    const { source, sink } = makeNativeLeaseProviders(true, true);
    await expect(
      transferEntry({
        source,
        sourceKey: "a",
        sink,
        destKey: "b",
        properties: await source.getProperties("a"),
        options: { retry: noBackoff },
        type: "copy",
        deleteSource: false,
      }),
    ).rejects.toThrow("Reopened handles no longer support the lease path");
  });
});
