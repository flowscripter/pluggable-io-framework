import type {
  ItemOfKind,
  PayloadKind,
  StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * A `StreamOpenerDecorator`: wraps a `StreamHandle` opener so the
 * underlying source is read at most once - the first call drains the
 * source while caching every item in memory; every subsequent call replays
 * the cached items without touching the source again.
 *
 * A plain `StreamHandle` only exposes a single one-shot `ReadableStream`, so
 * caching can't be a `StreamDecorator` operating on an already-open handle
 * (there would be nothing left to re-read on a second call) - it has to
 * intercept the *open* operation itself.
 */
export function locallyCached<K extends PayloadKind>(
  open: () => Promise<StreamHandle<K>>,
): () => Promise<StreamHandle<K>> {
  let cache: { handle: StreamHandle<K>; items: ItemOfKind<K>[] } | undefined;

  function replay(items: ItemOfKind<K>[]): ReadableStream<ItemOfKind<K>> {
    return new ReadableStream<ItemOfKind<K>>({
      start(controller) {
        for (const item of items) controller.enqueue(item);
        controller.close();
      },
    });
  }

  function describe(handle: StreamHandle<K>, stream: ReadableStream<ItemOfKind<K>>) {
    return {
      kind: handle.kind,
      stream,
      bounded: handle.bounded,
      payloadType: handle.payloadType,
    };
  }

  return async () => {
    if (cache) {
      return describe(cache.handle, replay(cache.items));
    }

    const handle = await open();
    const reader = (handle.stream as ReadableStream<ItemOfKind<K>>).getReader();
    const items: ItemOfKind<K>[] = [];

    const stream = new ReadableStream<ItemOfKind<K>>({
      async pull(controller) {
        const { done, value } = await reader.read();
        if (done) {
          cache = { handle, items };
          controller.close();
          return;
        }
        items.push(value);
        controller.enqueue(value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });

    return describe(handle, stream);
  };
}
