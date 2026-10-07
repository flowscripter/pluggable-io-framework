import { describe, expect, test } from "bun:test";
import {
  PayloadKind,
  type Item,
  type RangeReadable,
  type StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";
import { seekable } from "../../src/decorators/seekable.ts";

function streamOf(text: string): ReadableStream<Item<PayloadKind.Js>> {
  return new ReadableStream<Item<PayloadKind.Js>>({
    start(controller) {
      controller.enqueue({
        payload: { kind: PayloadKind.Js, data: new TextEncoder().encode(text) },
      });
      controller.close();
    },
  });
}

async function drain(handle: StreamHandle<PayloadKind.Js>): Promise<string> {
  const reader = (handle.stream as ReadableStream<Item<PayloadKind.Js>>).getReader();
  const parts: Uint8Array[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value.payload.data);
  }
  return new TextDecoder().decode(Buffer.concat(parts));
}

describe("seekable", () => {
  test("reads from the current position, then continues from a seeked offset", async () => {
    const content = "hello world";
    const handle: StreamHandle<PayloadKind.Js> & RangeReadable<PayloadKind.Js> = {
      kind: PayloadKind.Js,
      stream: streamOf(content),
      readRange: async (start: number) => streamOf(content.slice(start)),
    };

    const handleSeekable = seekable(handle);
    const reader = (handleSeekable.stream as ReadableStream<Item<PayloadKind.Js>>).getReader();

    const first = await reader.read();
    expect(new TextDecoder().decode(first.value?.payload.data)).toBe(content);

    await handleSeekable.seek(6);
    const second = await reader.read();
    expect(new TextDecoder().decode(second.value?.payload.data)).toBe("world");
  });

  test("cancelling the seekable stream cancels the current reader", async () => {
    let cancelled = false;
    const handle: StreamHandle<PayloadKind.Js> & RangeReadable<PayloadKind.Js> = {
      kind: PayloadKind.Js,
      stream: new ReadableStream<Item<PayloadKind.Js>>({
        cancel() {
          cancelled = true;
        },
      }),
      readRange: async () => streamOf(""),
    };
    await (seekable(handle).stream as ReadableStream<Item<PayloadKind.Js>>).cancel();
    expect(cancelled).toBe(true);
  });

  test("reads to the end of a seeked range", async () => {
    const handle: StreamHandle<PayloadKind.Js> & RangeReadable<PayloadKind.Js> = {
      kind: PayloadKind.Js,
      stream: streamOf("abc"),
      readRange: async (start: number) => streamOf("abc".slice(start)),
    };
    const decorated = seekable(handle);
    await decorated.seek(1);
    expect(await drain(decorated)).toBe("bc");
  });
});
