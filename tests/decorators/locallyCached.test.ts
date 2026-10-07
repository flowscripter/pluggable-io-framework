import { describe, expect, test } from "bun:test";
import {
  PayloadKind,
  type Item,
  type StreamHandle,
  type StreamOpenerDecorator,
} from "@flowscripter/pluggable-io-framework-api";
import { locallyCached } from "../../src/decorators/locallyCached.ts";

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

describe("locallyCached", () => {
  test("reads the source once, replaying the cache on subsequent opens", async () => {
    let openCalls = 0;
    const open = locallyCached<PayloadKind.Js>(async () => {
      openCalls += 1;
      return { kind: PayloadKind.Js, stream: streamOf("cached content") };
    });

    const first = await drain(await open());
    const second = await drain(await open());

    expect(first).toBe("cached content");
    expect(second).toBe("cached content");
    expect(openCalls).toBe(1);
  });

  test("does not cache until the source stream is fully drained", async () => {
    let openCalls = 0;
    const open = locallyCached<PayloadKind.Js>(async () => {
      openCalls += 1;
      return { kind: PayloadKind.Js, stream: streamOf("partial") };
    });

    const handle = await open();
    const reader = (handle.stream as ReadableStream<Item<PayloadKind.Js>>).getReader();
    await reader.read(); // read one chunk, don't drain to done

    await drain(await open());

    expect(openCalls).toBe(2);
  });

  test("is a StreamOpenerDecorator that keeps bounded and payloadType", async () => {
    const decorator: StreamOpenerDecorator<PayloadKind.Js> = locallyCached;
    const open = decorator(async () => ({
      kind: PayloadKind.Js,
      stream: streamOf("x"),
      bounded: true,
      payloadType: "bytes",
    }));
    const first = await open();
    expect(first.payloadType).toBe("bytes");
    await drain(first);
    const second = await open();
    expect(second.bounded).toBe(true);
    expect(second.payloadType).toBe("bytes");
  });

  test("cancelling the first stream cancels the source", async () => {
    let cancelled = false;
    const open = locallyCached<PayloadKind.Js>(async () => ({
      kind: PayloadKind.Js,
      stream: new ReadableStream<Item<PayloadKind.Js>>({
        cancel() {
          cancelled = true;
        },
      }),
    }));
    await ((await open()).stream as ReadableStream<Item<PayloadKind.Js>>).cancel();
    expect(cancelled).toBe(true);
  });
});
