import type {
  Part,
  PayloadKind,
  RangeReadable,
  StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * Splits a `RangeReadable` handle into parts of `partSize` bytes covering
 * `totalSize`. Each part's stream is opened with `readRange(start, end)`
 * (`end` exclusive) when the part is pulled. With `startOffset`, parts begin
 * at the part containing that offset.
 */
export async function* rangeReadableMultipartReader<K extends PayloadKind>(
  handle: StreamHandle<K> & RangeReadable<K>,
  totalSize: number,
  partSize: number,
  startOffset = 0,
): AsyncGenerator<Part<K>> {
  const partCount = Math.max(1, Math.ceil(totalSize / partSize));
  for (let index = Math.floor(startOffset / partSize); index < partCount; index += 1) {
    const start = index * partSize;
    const end = Math.min(start + partSize, totalSize);
    yield {
      index,
      offset: start,
      kind: handle.kind,
      stream: await handle.readRange(start, end),
      complete: async () => {},
    };
  }
}
