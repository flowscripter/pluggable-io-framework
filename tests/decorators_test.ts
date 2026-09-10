import { describe, expect, test } from "bun:test";
import {
  ChunkKind,
  type JsChunk,
  type RangeReadable,
  type StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";
import { locallyCached } from "../src/decorators/locallyCached.ts";
import { seekable } from "../src/decorators/seekable.ts";

function streamOf(text: string): ReadableStream<JsChunk> {
  return new ReadableStream<JsChunk>({
    start(controller) {
      controller.enqueue({ kind: ChunkKind.Js, data: new TextEncoder().encode(text) });
      controller.close();
    },
  });
}

async function drain(handle: StreamHandle<ChunkKind.Js>): Promise<string> {
  const reader = (handle.stream as ReadableStream<JsChunk>).getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value.data);
  }
  return new TextDecoder().decode(Buffer.concat(parts));
}

describe("seekable", () => {
  test("reads from the current position, then continues from a seeked offset", async () => {
    const content = "hello world";
    const handle: StreamHandle<ChunkKind.Js> & RangeReadable<ChunkKind.Js> = {
      kind: ChunkKind.Js,
      stream: streamOf(content),
      readRange: async (start: number) => streamOf(content.slice(start)),
    };

    const handleSeekable = seekable(handle);
    const reader = (handleSeekable.stream as ReadableStream<JsChunk>).getReader();

    const first = await reader.read();
    expect(new TextDecoder().decode(first.value?.data)).toBe(content);

    await handleSeekable.seek(6);
    const second = await reader.read();
    expect(new TextDecoder().decode(second.value?.data)).toBe("world");
  });
});

describe("locallyCached", () => {
  test("reads the source once, replaying the cache on subsequent opens", async () => {
    let openCalls = 0;
    const open = locallyCached<ChunkKind.Js>(async () => {
      openCalls += 1;
      return { kind: ChunkKind.Js, stream: streamOf("cached content") };
    });

    const first = await drain(await open());
    const second = await drain(await open());

    expect(first).toBe("cached content");
    expect(second).toBe("cached content");
    expect(openCalls).toBe(1);
  });

  test("does not cache until the source stream is fully drained", async () => {
    let openCalls = 0;
    const open = locallyCached<ChunkKind.Js>(async () => {
      openCalls += 1;
      return { kind: ChunkKind.Js, stream: streamOf("partial") };
    });

    const handle = await open();
    const reader = (handle.stream as ReadableStream<JsChunk>).getReader();
    await reader.read(); // read one chunk, don't drain to done

    await drain(await open());

    expect(openCalls).toBe(2);
  });
});
