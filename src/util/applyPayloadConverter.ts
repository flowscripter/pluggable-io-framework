import {
  adaptReadableStream,
  type Item,
  type PayloadConverter,
  type PayloadKind,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * Converts every item of `stream` for a sink of `toKind`. With a converter,
 * each item goes through `converter.convert`, including when the kinds match
 * and only the memory domain differs. Without one, a kind mismatch fails on
 * the first item.
 */
export function applyPayloadConverter(
  stream: ReadableStream<Item>,
  fromKind: PayloadKind,
  toKind: PayloadKind,
  converter: PayloadConverter | undefined,
): ReadableStream<Item> {
  if (!converter) {
    return adaptReadableStream(stream, fromKind, toKind);
  }
  const reader = stream.getReader();
  return new ReadableStream<Item>({
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) {
        controller.close();
        return;
      }
      controller.enqueue(converter.convert(value));
    },
    cancel(reason) {
      return reader.cancel(reason);
    },
  });
}
