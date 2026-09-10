import { describe, expect, test } from "bun:test";
import {
  ChunkKind,
  type ChunkRef,
  type IOProvider,
  type JsChunk,
  type Part,
  type PartSizeConstraints,
} from "@flowscripter/pluggable-io-framework-api";
import { copy } from "../src/copyMove.ts";

function makeNegotiatingProvider(
  files: Map<string, Uint8Array>,
  recordedPartSizes: number[],
  constraints?: (totalSize: number) => PartSizeConstraints,
): IOProvider {
  return {
    kind: ChunkKind.Js,
    async [Symbol.asyncDispose]() {},
    async *list() {},
    async getProperties(path: string) {
      const data = files.get(path);
      return { size: data?.byteLength, lastModified: undefined, isFolder: false, properties: {} };
    },
    async setProperties() {},
    async delete(path: string) {
      files.delete(path);
    },
    async getReadableStream(path: string) {
      const data = files.get(path) ?? new Uint8Array();
      return {
        kind: ChunkKind.Js,
        stream: new ReadableStream<JsChunk>({
          start: (c) => (c.enqueue({ kind: ChunkKind.Js, data }), c.close()),
        }),
      };
    },
    async getWritableStream(path: string) {
      const chunks: Uint8Array[] = [];
      return {
        kind: ChunkKind.Js,
        stream: new WritableStream<JsChunk>({
          write: (chunk) => {
            chunks.push(chunk.data);
          },
          close: () => {
            files.set(path, Buffer.concat(chunks));
          },
        }),
      };
    },
    getPartSizeConstraints: constraints,
    getMultipartReader: async function* (path: string, partSize: number) {
      recordedPartSizes.push(partSize);
      const data = files.get(path) ?? new Uint8Array();
      let offset = 0;
      let index = 0;
      while (offset < data.byteLength) {
        const end = Math.min(offset + partSize, data.byteLength);
        const partData = data.subarray(offset, end);
        const part: Part = {
          index,
          offset,
          kind: ChunkKind.Js,
          stream: new ReadableStream<JsChunk>({
            start: (c) => (c.enqueue({ kind: ChunkKind.Js, data: partData }), c.close()),
          }),
          complete: async () => {},
        };
        yield part;
        offset = end;
        index += 1;
      }
    },
    getMultipartWriter(path: string, partSize: number) {
      recordedPartSizes.push(partSize);
      return {
        async write(parts: AsyncIterable<Part>) {
          const collected: { offset: number; data: Uint8Array }[] = [];
          for await (const part of parts) {
            const reader = (part.stream as ReadableStream<ChunkRef>).getReader();
            const chunks: Uint8Array[] = [];
            for (;;) {
              const { done, value } = await reader.read();
              if (done) break;
              if (value.kind === ChunkKind.Js) chunks.push(value.data);
            }
            collected.push({ offset: part.offset, data: Buffer.concat(chunks) });
            await part.complete();
          }
          collected.sort((a, b) => a.offset - b.offset);
          files.set(path, Buffer.concat(collected.map((c) => c.data)));
        },
      };
    },
  };
}

describe("part-size negotiation", () => {
  test("defaults to 8MB when neither side declares constraints", async () => {
    const data = new Uint8Array(200);
    const sourceFiles = new Map([["a.bin", data]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const sourceSizes: number[] = [];
    const sinkSizes: number[] = [];
    const source = makeNegotiatingProvider(sourceFiles, sourceSizes);
    const sink = makeNegotiatingProvider(sinkFiles, sinkSizes);

    await copy(source, "a.bin", sink, "b.bin", { multipartThreshold: 1 });

    expect(sourceSizes[0]).toBe(8 * 1024 * 1024);
    expect(sinkSizes[0]).toBe(8 * 1024 * 1024);
  });

  test("reconciles source/sink bounds and honours the larger minPartSize", async () => {
    const data = new Uint8Array(200);
    const sourceFiles = new Map([["a.bin", data]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const sourceSizes: number[] = [];
    const sinkSizes: number[] = [];
    const source = makeNegotiatingProvider(sourceFiles, sourceSizes, () => ({
      minPartSize: 5 * 1024 * 1024,
      maxPartSize: Infinity,
      maxParts: 10000,
      defaultPartSize: 5 * 1024 * 1024,
    }));
    const sink = makeNegotiatingProvider(sinkFiles, sinkSizes, () => ({
      minPartSize: 0,
      maxPartSize: Infinity,
      maxParts: 10000,
      defaultPartSize: 8 * 1024 * 1024,
    }));

    await copy(source, "a.bin", sink, "b.bin", { multipartThreshold: 1 });

    expect(sourceSizes[0]).toBe(8 * 1024 * 1024);
  });

  test("bumps part size up when maxParts would otherwise be exceeded for a huge file", async () => {
    const totalSize = 200_000;
    const data = new Uint8Array(totalSize);
    const sourceFiles = new Map([["a.bin", data]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const sourceSizes: number[] = [];
    const sinkSizes: number[] = [];
    const source = makeNegotiatingProvider(sourceFiles, sourceSizes, () => ({
      minPartSize: 5,
      maxPartSize: Infinity,
      maxParts: 4, // forces part size >= ceil(200000 / 4) = 50000
      defaultPartSize: 10,
    }));
    const sink = makeNegotiatingProvider(sinkFiles, sinkSizes, () => ({
      minPartSize: 0,
      maxPartSize: Infinity,
      maxParts: 10000,
      defaultPartSize: 10,
    }));

    await copy(source, "a.bin", sink, "b.bin", { multipartThreshold: 1 });

    expect(sourceSizes[0]).toBe(50_000);
  });

  test("falls back to plain streaming when bounds are mutually infeasible", async () => {
    const sourceFiles = new Map([["a.bin", new Uint8Array(200)]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const sourceSizes: number[] = [];
    const sinkSizes: number[] = [];
    const source = makeNegotiatingProvider(sourceFiles, sourceSizes, () => ({
      minPartSize: 100,
      maxPartSize: 200,
      maxParts: 10000,
      defaultPartSize: 100,
    }));
    const sink = makeNegotiatingProvider(sinkFiles, sinkSizes, () => ({
      minPartSize: 0,
      maxPartSize: 50, // below source's minPartSize - infeasible
      maxParts: 10000,
      defaultPartSize: 10,
    }));

    await copy(source, "a.bin", sink, "b.bin", { multipartThreshold: 1 });

    expect(sourceSizes.length).toBe(0); // getMultipartReader never called
    expect(sinkSizes.length).toBe(0); // getMultipartWriter never called
    expect(sinkFiles.get("b.bin")?.byteLength).toBe(200); // streamed successfully instead
  });
});
