import {
  type IOProvider,
  type Item,
  type Part,
  type RangeReadable,
  type StreamHandle,
  TransientIOError,
} from "@flowscripter/pluggable-io-framework-api";
import type { ConcurrencyLimiter } from "../concurrency/ConcurrencyLimiter.ts";
import { mapAsyncIterableConcurrently } from "../concurrency/mapAsyncIterableConcurrently.ts";
import { backoff, DEFAULT_RETRY } from "../retry/RetryOptions.ts";
import { createAbortError } from "../util/abort.ts";
import { applyPayloadConverter } from "../util/applyPayloadConverter.ts";
import { itemLength } from "../util/itemLength.ts";
import type { EntryContext, EntryOutcome } from "./EntryContext.ts";
import { rangeReadableMultipartReader } from "./rangeReadableMultipartReader.ts";

export interface MultipartTransferInput {
  readonly readable: StreamHandle & RangeReadable;
  readonly sink: IOProvider;
  readonly writer: { write(parts: AsyncIterable<Part>): Promise<void> };
  readonly totalBytes: number;
  readonly partSize: number;
  readonly limiter: ConcurrencyLimiter;
  readonly context: EntryContext;
}

/**
 * Reads parts with `readRange` and hands them to the sink's multipart
 * writer, bounded by `limiter`. A part whose read fails with a
 * `TransientIOError` is re-read on its own, up to `retry.maxRetries` times,
 * within the same upload. `stop` ends the transfer after the parts already
 * started; `signal` aborts it.
 */
export async function multipartTransfer(input: MultipartTransferInput): Promise<EntryOutcome> {
  const { readable, sink, writer, totalBytes, partSize, limiter, context } = input;
  const { options, hooks, operationId, type } = context;
  const retry = options.retry ?? DEFAULT_RETRY;
  let bytes = 0;
  let items = 0;
  let stopped = false;

  async function readPart(sourcePart: Part): Promise<Item[]> {
    const partOperationId = crypto.randomUUID();
    const end = Math.min(sourcePart.offset + partSize, totalBytes);
    let stream = sourcePart.stream as ReadableStream<Item>;
    let attempt = 0;
    for (;;) {
      const reader = applyPayloadConverter(
        stream,
        readable.kind,
        sink.kind,
        options.converter,
      ).getReader();
      const partItems: Item[] = [];
      let partBytes = 0;
      try {
        for (;;) {
          if (options.signal?.aborted) {
            await reader.cancel().catch(() => {});
            throw createAbortError();
          }
          const { done, value } = await reader.read();
          if (done) break;
          partItems.push(value);
          partBytes += itemLength(value);
          hooks.onProgress?.({
            operationId: partOperationId,
            parentOperationId: operationId,
            type,
            bytesProcessed: partBytes,
          });
        }
        bytes += partBytes;
        items += partItems.length;
        hooks.onProgress?.({ operationId, type, bytesProcessed: bytes, totalBytes });
        return partItems;
      } catch (error) {
        if (!(error instanceof TransientIOError) || attempt >= retry.maxRetries) {
          throw error;
        }
        attempt += 1;
        await backoff(retry, attempt);
        stream = (await readable.readRange(sourcePart.offset, end)) as ReadableStream<Item>;
      }
    }
  }

  async function transferPart(sourcePart: Part): Promise<Part> {
    const partItems = await readPart(sourcePart);
    return {
      index: sourcePart.index,
      offset: sourcePart.offset,
      kind: sink.kind,
      stream: new ReadableStream<Item>({
        start(controller) {
          for (const item of partItems) controller.enqueue(item);
          controller.close();
        },
      }),
      complete: () => sourcePart.complete(),
    };
  }

  async function* sourceParts(): AsyncGenerator<Part> {
    for await (const part of rangeReadableMultipartReader(readable, totalBytes, partSize)) {
      if (options.signal?.aborted) throw createAbortError();
      if (options.stop?.aborted) {
        stopped = true;
        await (part.stream as ReadableStream<Item>).cancel().catch(() => {});
        return;
      }
      yield part;
    }
  }

  await writer.write(mapAsyncIterableConcurrently(sourceParts(), transferPart, limiter));
  return { bytes, items, stopped };
}
