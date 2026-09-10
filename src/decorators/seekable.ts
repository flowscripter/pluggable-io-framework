import type {
  ChunkKind,
  ChunkOfKind,
  RangeReadable,
  Seekable,
  StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * Wraps a handle that already supports {@link RangeReadable} (arbitrary
 * byte-range reads) with a {@link Seekable} capability: a single logical
 * stream whose read position can be jumped via `seek(offset)`, rather than
 * requiring the caller to open a fresh stream per range.
 *
 * `seek` must not be called while a read from the current position is still
 * in flight - like any seekable stream, seeking and reading are sequential,
 * not concurrent, operations on the same handle.
 */
export function seekable<K extends ChunkKind>(
  handle: StreamHandle<K> & RangeReadable<K>,
): StreamHandle<K> & Seekable {
  let reader = (handle.stream as ReadableStream<ChunkOfKind<K>>).getReader();

  const stream = new ReadableStream<ChunkOfKind<K>>(
    {
      async pull(controller) {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        controller.enqueue(value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    },
    // Default queuing strategy pre-fetches one chunk ahead of what the
    // consumer has actually asked for, which would call pull() again -
    // hitting "done" on the pre-seek reader and closing the stream - before
    // a seek() issued between two explicit reads ever runs. highWaterMark 0
    // makes pull() fire only for an explicit read, never speculatively.
    { highWaterMark: 0 },
  );

  return {
    kind: handle.kind,
    stream,
    async seek(offset: number) {
      const ranged = await handle.readRange(offset, Number.MAX_SAFE_INTEGER);
      reader = ranged.getReader();
    },
  };
}
