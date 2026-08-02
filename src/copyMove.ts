import {
  adaptReadableStream,
  identityChunkConverter,
  type ChunkConverter,
  type ChunkRef,
  type IOProvider,
  type Part,
  type TelemetryHooks,
} from "@flowscripter/pluggable-io-framework-api";
import { chunkLength } from "./chunkLength.ts";

export interface TransferOptions {
  readonly telemetry?: TelemetryHooks;
  /** Minimum file size (bytes) before multipart transfer is attempted over plain streaming. */
  readonly multipartThreshold?: number;
  /**
   * Required only when `source.kind !== sink.kind` - pure-TS code can only
   * convert chunks already of the target kind (see
   * `identityChunkConverter`); a real js<->native conversion needs an
   * FFI-capable converter supplied by a runtime-specific package.
   */
  readonly chunkConverter?: ChunkConverter;
}

const DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024;

async function streamingTransfer(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  operationId: string,
  options: TransferOptions,
  totalBytes: number | undefined,
): Promise<void> {
  const readable = await source.getReadableStream(sourcePath);
  const writable = await sink.getWritableStream(destPath);
  // The source/sink kind mismatch is decided ONCE here, not per chunk.
  const adaptedStream = adaptReadableStream(
    readable.stream as ReadableStream<ChunkRef>,
    readable.kind,
    writable.kind,
    options.chunkConverter ?? identityChunkConverter,
  );
  const reader = adaptedStream.getReader();
  const writer = (writable.stream as WritableStream<ChunkRef>).getWriter();
  let bytesProcessed = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      await writer.write(value);
      bytesProcessed += chunkLength(value);
      options.telemetry?.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
    }
    await writer.close();
  } catch (error) {
    await writer.abort(error);
    throw error;
  }
}

async function multipartTransfer(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  operationId: string,
  options: TransferOptions,
  totalBytes: number | undefined,
): Promise<void> {
  let bytesProcessed = 0;
  const writer = sink.getMultipartWriter(destPath);
  async function* transferParts(): AsyncIterable<Part> {
    for await (const part of source.getMultipartReader(sourcePath)) {
      // Kind mismatch decided once per part, not once per chunk within it.
      const adaptedStream = adaptReadableStream(
        part.stream as ReadableStream<ChunkRef>,
        part.kind,
        sink.kind,
        options.chunkConverter ?? identityChunkConverter,
      );
      const reader = adaptedStream.getReader();
      const chunks: ChunkRef[] = [];
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        bytesProcessed += chunkLength(value);
        options.telemetry?.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
      }
      yield {
        index: part.index,
        offset: part.offset,
        kind: sink.kind,
        stream: new ReadableStream<ChunkRef>({
          start(controller) {
            for (const chunk of chunks) controller.enqueue(chunk);
            controller.close();
          },
        }),
        complete: () => part.complete(),
      };
    }
  }
  await writer.write(transferParts());
}

function canUseMultipart(
  source: IOProvider,
  sink: IOProvider,
  size: number | undefined,
  threshold: number,
): boolean {
  return (
    size !== undefined &&
    size >= threshold &&
    typeof source.getMultipartReader === "function" &&
    typeof sink.getMultipartWriter === "function"
  );
}

/**
 * Copies `sourcePath` on `source` to `destPath` on `sink`.
 *
 * Uses `source.directCopy` when `source.canDirectTransfer?.(sink)` reports
 * eligibility (same-provider direct transfer). Otherwise falls back to
 * multipart transfer (when both sides support it and size crosses
 * `options.multipartThreshold`) or plain streaming.
 */
export async function copy(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions = {},
): Promise<void> {
  const operationId = crypto.randomUUID();
  if (source.canDirectTransfer?.(sink) && source.directCopy) {
    await source.directCopy(sourcePath, destPath);
    return;
  }
  const properties = await source.getProperties(sourcePath);
  const threshold = options.multipartThreshold ?? DEFAULT_MULTIPART_THRESHOLD;
  if (canUseMultipart(source, sink, properties.size, threshold)) {
    await multipartTransfer(
      source,
      sourcePath,
      sink,
      destPath,
      operationId,
      options,
      properties.size,
    );
    return;
  }
  await streamingTransfer(
    source,
    sourcePath,
    sink,
    destPath,
    operationId,
    options,
    properties.size,
  );
}

/**
 * Moves `sourcePath` on `source` to `destPath` on `sink`. Uses
 * `source.directMove` when eligible, otherwise performs a {@link copy}
 * followed by deleting the source item.
 */
export async function move(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions = {},
): Promise<void> {
  if (source.canDirectTransfer?.(sink) && source.directMove) {
    await source.directMove(sourcePath, destPath);
    return;
  }
  await copy(source, sourcePath, sink, destPath, options);
  await source.delete(sourcePath);
}
