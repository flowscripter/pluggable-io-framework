import { describe, expect, test } from "bun:test";
import {
  ChunkKind,
  type ChunkRef,
  type IOProvider,
  type JsChunk,
  type Part,
} from "@flowscripter/pluggable-io-framework-api";
import { copy, move } from "../src/copyMove.ts";

function makeStreamingProvider(
  files: Map<string, Uint8Array>,
  id: string,
  kind: ChunkKind = ChunkKind.Js,
): IOProvider {
  return {
    kind,
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
        kind,
        stream: new ReadableStream<JsChunk>({
          start(controller) {
            controller.enqueue({ kind: ChunkKind.Js, data });
            controller.close();
          },
        }),
      };
    },
    async getWritableStream(path: string) {
      const chunks: Uint8Array[] = [];
      return {
        kind,
        stream: new WritableStream<JsChunk>({
          write(chunk) {
            chunks.push(chunk.data);
          },
          close() {
            files.set(path, Buffer.concat(chunks));
          },
        }),
      };
    },
    getMultipartReader: async function* () {},
    getMultipartWriter() {
      return { write: async () => {} };
    },
    canDirectTransfer(other: IOProvider) {
      return (other as unknown as { id?: string }).id === id;
    },
    async directCopy() {
      throw new Error("directCopy should not be reachable in this test provider");
    },
    ...({ id } as { id: string }),
  };
}

function makeMultipartProvider(files: Map<string, Uint8Array>): IOProvider {
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
    getMultipartReader: async function* (path: string) {
      const data = files.get(path) ?? new Uint8Array();
      const half = Math.ceil(data.byteLength / 2);
      const partsData = [data.subarray(0, half), data.subarray(half)];
      for (let index = 0; index < partsData.length; index += 1) {
        const partData = partsData[index] as Uint8Array;
        const part: Part = {
          index,
          offset: index === 0 ? 0 : half,
          kind: ChunkKind.Js,
          stream: new ReadableStream<JsChunk>({
            start: (c) => (c.enqueue({ kind: ChunkKind.Js, data: partData }), c.close()),
          }),
          complete: async () => {},
        };
        yield part;
      }
    },
    getMultipartWriter(path: string) {
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

describe("copy", () => {
  test("streaming fallback copies bytes between providers", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello world")]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const source = makeStreamingProvider(sourceFiles, "source");
    const sink = makeStreamingProvider(sinkFiles, "sink");

    await copy(source, "a.txt", sink, "b.txt");

    expect(new TextDecoder().decode(sinkFiles.get("b.txt"))).toBe("hello world");
  });

  test("reports progress via telemetry with a stable operationId", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello world")]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const source = makeStreamingProvider(sourceFiles, "source");
    const sink = makeStreamingProvider(sinkFiles, "sink");

    const events: { operationId: string; bytesProcessed: number }[] = [];
    await copy(source, "a.txt", sink, "b.txt", {
      telemetry: { onProgress: (event) => events.push(event) },
    });

    expect(events.length).toBeGreaterThan(0);
    expect(new Set(events.map((e) => e.operationId)).size).toBe(1);
    expect(events.at(-1)?.bytesProcessed).toBe("hello world".length);
  });

  test("uses directCopy when canDirectTransfer reports eligibility", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello")]]);
    const source: IOProvider = {
      ...makeStreamingProvider(sourceFiles, "same"),
      canDirectTransfer: () => true,
      directCopy: async () => {
        directCopyCalled = true;
      },
    };
    let directCopyCalled = false;
    const sink = makeStreamingProvider(new Map(), "same");

    await copy(source, "a.txt", sink, "b.txt");

    expect(directCopyCalled).toBe(true);
  });

  test("uses multipart transfer when both sides support it and size crosses threshold", async () => {
    const sourceFiles = new Map([
      ["a.txt", new TextEncoder().encode("hello world, this is a test payload")],
    ]);
    const sinkFiles = new Map<string, Uint8Array>();
    const source = makeMultipartProvider(sourceFiles);
    const sink = makeMultipartProvider(sinkFiles);

    await copy(source, "a.txt", sink, "b.txt", { multipartThreshold: 1 });

    expect(new TextDecoder().decode(sinkFiles.get("b.txt"))).toBe(
      "hello world, this is a test payload",
    );
  });

  test("throws when source/sink kinds differ and no converter is supplied", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello")]]);
    const source = makeStreamingProvider(sourceFiles, "source");
    const sink = makeStreamingProvider(new Map(), "sink", ChunkKind.Native);

    let threw = false;
    try {
      await copy(source, "a.txt", sink, "b.txt");
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });

  test("uses the supplied chunkConverter exactly once per differing-kind link", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello")]]);
    const source = makeStreamingProvider(sourceFiles, "source");
    const sinkFiles = new Map<string, Uint8Array>();
    const sink = makeStreamingProvider(sinkFiles, "sink", ChunkKind.Native);

    let converterCalls = 0;
    await copy(source, "a.txt", sink, "b.txt", {
      chunkConverter: (chunk, toKind) => {
        converterCalls += 1;
        expect(toKind).toBe(ChunkKind.Native);
        return chunk;
      },
    });

    expect(converterCalls).toBe(1);
    expect(new TextDecoder().decode(sinkFiles.get("b.txt"))).toBe("hello");
  });
});

describe("move", () => {
  test("streaming fallback copies then deletes source", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello")]]);
    const sinkFiles = new Map<string, Uint8Array>();
    const source = makeStreamingProvider(sourceFiles, "source");
    const sink = makeStreamingProvider(sinkFiles, "sink");

    await move(source, "a.txt", sink, "b.txt");

    expect(sourceFiles.has("a.txt")).toBe(false);
    expect(new TextDecoder().decode(sinkFiles.get("b.txt"))).toBe("hello");
  });

  test("uses directMove when canDirectTransfer reports eligibility", async () => {
    const sourceFiles = new Map([["a.txt", new TextEncoder().encode("hello")]]);
    let directMoveCalled = false;
    const source: IOProvider = {
      ...makeStreamingProvider(sourceFiles, "same"),
      canDirectTransfer: () => true,
      directMove: async () => {
        directMoveCalled = true;
      },
    };
    const sink = makeStreamingProvider(new Map(), "same");

    await move(source, "a.txt", sink, "b.txt");

    expect(directMoveCalled).toBe(true);
    expect(sourceFiles.has("a.txt")).toBe(true); // directMove is a stub in this test - it doesn't actually delete
  });
});
