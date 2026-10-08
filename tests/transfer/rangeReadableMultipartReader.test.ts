import { describe, expect, test } from "bun:test";
import {
  type Item,
  isRangeReadable,
  type PayloadKind,
} from "@flowscripter/pluggable-io-framework-api";
import { rangeReadableMultipartReader } from "../../src/transfer/rangeReadableMultipartReader.ts";
import { makeMemoryProvider, makeStore } from "../fixtures/memoryProvider.ts";

async function text(stream: ReadableStream<Item<PayloadKind.Js>>): Promise<string> {
  const chunks: Uint8Array[] = [];
  const reader = stream.getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value.payload.data);
  }
  return Buffer.concat(chunks).toString();
}

describe("rangeReadableMultipartReader", () => {
  test("splits a handle into ranged parts covering the whole size", async () => {
    const provider = makeMemoryProvider(makeStore({ a: "0123456789" }));
    const handle = await provider.getReadableStream("a");
    if (!isRangeReadable(handle)) throw new Error("expected RangeReadable");
    const parts: { index: number; offset: number; text: string }[] = [];
    for await (const part of rangeReadableMultipartReader(handle, 10, 4)) {
      parts.push({
        index: part.index,
        offset: part.offset,
        text: await text(part.stream as ReadableStream<Item<PayloadKind.Js>>),
      });
      await part.complete();
    }
    expect(parts).toEqual([
      { index: 0, offset: 0, text: "0123" },
      { index: 1, offset: 4, text: "4567" },
      { index: 2, offset: 8, text: "89" },
    ]);
  });

  test("starts at the part containing startOffset", async () => {
    const provider = makeMemoryProvider(makeStore({ a: "0123456789" }));
    const handle = await provider.getReadableStream("a");
    if (!isRangeReadable(handle)) throw new Error("expected RangeReadable");
    const offsets: number[] = [];
    for await (const part of rangeReadableMultipartReader(handle, 10, 4, 5)) {
      offsets.push(part.offset);
    }
    expect(offsets).toEqual([4, 8]);
  });

  test("yields a single empty part for an empty entry", async () => {
    const provider = makeMemoryProvider(makeStore({ a: "" }));
    const handle = await provider.getReadableStream("a");
    if (!isRangeReadable(handle)) throw new Error("expected RangeReadable");
    const offsets: number[] = [];
    for await (const part of rangeReadableMultipartReader(handle, 0, 4)) offsets.push(part.offset);
    expect(offsets).toEqual([0]);
  });
});
