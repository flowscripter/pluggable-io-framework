import { describe, expect, test } from "bun:test";
import { copy } from "../../src/transfer/copyMove.ts";
import { negotiatePartSize } from "../../src/transfer/negotiatePartSize.ts";
import {
  makeMemoryProvider,
  makeStore,
  type MemoryProviderOptions,
} from "../fixtures/memoryProvider.ts";
import { entry } from "../fixtures/targets.ts";

function setup(
  size: number,
  source: MemoryProviderOptions["partSizeConstraints"],
  sink: MemoryProviderOptions["partSizeConstraints"],
) {
  const sourceStore = makeStore();
  sourceStore.files.set("a.bin", new Uint8Array(size));
  const sinkStore = makeStore();
  const sinkSizes: number[] = [];
  return {
    sinkStore,
    sinkSizes,
    source: makeMemoryProvider(sourceStore, { partSizeConstraints: source }),
    sink: makeMemoryProvider(sinkStore, {
      multipart: true,
      partSizeConstraints: sink,
      recordedPartSizes: sinkSizes,
    }),
  };
}

describe("part-size negotiation", () => {
  test("defaults to 8MB when neither side declares constraints", async () => {
    const { source, sink, sinkSizes } = setup(200, undefined, undefined);
    expect(negotiatePartSize(source, sink, 200)).toBe(8 * 1024 * 1024);
    await copy(source, entry("a.bin"), sink, entry("b.bin"), { multipartThreshold: 1 });
    expect(sinkSizes[0]).toBe(8 * 1024 * 1024);
  });

  test("reconciles source/sink bounds and honours the larger minPartSize", () => {
    const { source, sink } = setup(
      200,
      () => ({
        minPartSize: 5 * 1024 * 1024,
        maxPartSize: Infinity,
        maxParts: 10000,
        defaultPartSize: 5 * 1024 * 1024,
      }),
      () => ({
        minPartSize: 0,
        maxPartSize: Infinity,
        maxParts: 10000,
        defaultPartSize: 8 * 1024 * 1024,
      }),
    );
    expect(negotiatePartSize(source, sink, 200)).toBe(8 * 1024 * 1024);
  });

  test("bumps part size up when maxParts would otherwise be exceeded for a huge file", async () => {
    const { source, sink, sinkSizes, sinkStore } = setup(
      200_000,
      // forces part size >= ceil(200000 / 4) = 50000
      () => ({ minPartSize: 5, maxPartSize: Infinity, maxParts: 4, defaultPartSize: 10 }),
      () => ({ minPartSize: 0, maxPartSize: Infinity, maxParts: 10000, defaultPartSize: 10 }),
    );
    await copy(source, entry("a.bin"), sink, entry("b.bin"), { multipartThreshold: 1 });
    expect(sinkSizes[0]).toBe(50_000);
    expect(sinkStore.files.get("b.bin")?.byteLength).toBe(200_000);
  });

  test("falls back to plain streaming when bounds are mutually infeasible", async () => {
    const { source, sink, sinkSizes, sinkStore } = setup(
      200,
      () => ({ minPartSize: 100, maxPartSize: 200, maxParts: 10000, defaultPartSize: 100 }),
      // below the source's minPartSize - infeasible
      () => ({ minPartSize: 0, maxPartSize: 50, maxParts: 10000, defaultPartSize: 10 }),
    );
    expect(negotiatePartSize(source, sink, 200)).toBeUndefined();
    const result = await copy(source, entry("a.bin"), sink, entry("b.bin"), {
      multipartThreshold: 1,
    });
    expect(sinkSizes.length).toBe(0);
    expect(sinkStore.files.get("b.bin")?.byteLength).toBe(200);
    expect(result.path).toBe("js -> js, stream");
  });
});
