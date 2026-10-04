import { describe, expect, test } from "bun:test";
import {
  type Item,
  type PayloadConverterExtension,
  PayloadKind,
} from "@flowscripter/pluggable-io-framework-api";
import { applyPayloadConverter } from "../../src/util/applyPayloadConverter.ts";
import { jsItem } from "../fixtures/memoryProvider.ts";

function streamOf(...items: Item[]): ReadableStream<Item> {
  return new ReadableStream<Item>({
    start(controller) {
      for (const item of items) controller.enqueue(item);
      controller.close();
    },
  });
}

describe("applyPayloadConverter", () => {
  test("passes a same-kind stream through without a converter", () => {
    const stream = streamOf(jsItem("a"));
    expect(applyPayloadConverter(stream, PayloadKind.Js, PayloadKind.Js, undefined)).toBe(stream);
  });

  test("fails a kind mismatch without a converter", async () => {
    const reader = applyPayloadConverter(
      streamOf(jsItem("a")),
      PayloadKind.Js,
      PayloadKind.Native,
      undefined,
    ).getReader();
    await expect(reader.read()).rejects.toThrow();
  });

  test("applies a converter to every item even when kinds match", async () => {
    let calls = 0;
    const converter: PayloadConverterExtension = {
      from: { kind: PayloadKind.Js },
      to: { kind: PayloadKind.Js },
      cost: 1,
      convert: (item) => {
        calls += 1;
        return item;
      },
    };
    const converted = applyPayloadConverter(
      streamOf(jsItem("a"), jsItem("b")),
      PayloadKind.Js,
      PayloadKind.Js,
      converter,
    );
    const reader = converted.getReader();
    while (!(await reader.read()).done) {
      // drain
    }
    expect(calls).toBe(2);
    await applyPayloadConverter(streamOf(), PayloadKind.Js, PayloadKind.Js, converter).cancel();
  });
});
