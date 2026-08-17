import type {
  ChunkKind,
  ChunkOfKind,
  StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * Wraps a `StreamHandle` factory (e.g. `() => provider.getReadableStream(path)`)
 * so the underlying source is read at most once - the first call drains the
 * source while caching every chunk in memory; every subsequent call replays
 * the cached chunks without touching the source again.
 *
 * A plain `StreamHandle` only exposes a single one-shot `ReadableStream`, so
 * caching can't be a `StreamDecorator<K, C>` operating on an already-open
 * handle (there would be nothing left to re-read on a second call) - it has
 * to intercept the *open* operation itself, provider-agnostic regardless of
 * what kind of source is behind it.
 */
export function locallyCached<K extends ChunkKind>(
  open: () => Promise<StreamHandle<K>>,
): () => Promise<StreamHandle<K>> {
  let cache: { kind: K; chunks: ChunkOfKind<K>[] } | undefined;

  function replay(chunks: ChunkOfKind<K>[]): ReadableStream<ChunkOfKind<K>> {
    return new ReadableStream<ChunkOfKind<K>>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk);
        controller.close();
      },
    });
  }

  return async () => {
    if (cache) {
      return { kind: cache.kind, stream: replay(cache.chunks) };
    }

    const handle = await open();
    const reader = (handle.stream as ReadableStream<ChunkOfKind<K>>).getReader();
    const chunks: ChunkOfKind<K>[] = [];

    const stream = new ReadableStream<ChunkOfKind<K>>({
      async pull(controller) {
        const { done, value } = await reader.read();
        if (done) {
          cache = { kind: handle.kind, chunks };
          controller.close();
          return;
        }
        chunks.push(value);
        controller.enqueue(value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });

    return { kind: handle.kind, stream };
  };
}
