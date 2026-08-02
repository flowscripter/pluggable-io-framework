import type {
  ChunkRef,
  IOProvider,
  Part,
  TelemetryHooks,
} from "@flowscripter/pluggable-io-framework-api";
import { chunkLength } from "./chunkLength.ts";

export interface TransferOptions {
  readonly telemetry?: TelemetryHooks;
  /** Minimum file size (bytes) before multipart transfer is attempted over plain streaming. */
  readonly multipartThreshold?: number;
}

const DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024;

async function streamingTransfer(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  operationId: string,
  telemetry: TelemetryHooks | undefined,
  totalBytes: number | undefined,
): Promise<void> {
  const readable = await source.getReadableStream(sourcePath);
  const writable = await sink.getWritableStream(destPath);
  const reader = (readable.stream as ReadableStream<ChunkRef>).getReader();
  const writer = (writable.stream as WritableStream<ChunkRef>).getWriter();
  let bytesProcessed = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      await writer.write(value);
      bytesProcessed += chunkLength(value);
      telemetry?.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
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
  telemetry: TelemetryHooks | undefined,
  totalBytes: number | undefined,
): Promise<void> {
  let bytesProcessed = 0;
  const writer = sink.getMultipartWriter(destPath);
  async function* transferParts(): AsyncIterable<Part> {
    for await (const part of source.getMultipartReader(sourcePath)) {
      const reader = (part.stream as ReadableStream<ChunkRef>).getReader();
      const chunks: ChunkRef[] = [];
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
        bytesProcessed += chunkLength(value);
        telemetry?.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
      }
      yield {
        index: part.index,
        offset: part.offset,
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
      options.telemetry,
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
    options.telemetry,
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
