import {
  BYTES_PAYLOAD_TYPE,
  type EntryProperties,
  type IOProvider,
  isBufferProvider,
  isFillReadable,
  isRangeReadable,
  isResumableWritable,
  type Item,
  type MultipartWriter,
  PermanentIOError,
  type StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";
import { defaultConcurrencyLimiter } from "../concurrency/ConcurrencyLimiter.ts";
import { DEFAULT_RETRY } from "../retry/RetryOptions.ts";
import { withRetry } from "../retry/withRetry.ts";
import { throwIfAborted } from "../util/abort.ts";
import { describeConverter } from "../util/describePath.ts";
import { withParentOperationId } from "../util/withParentOperationId.ts";
import type { EntryContext, EntryOutcome } from "./EntryContext.ts";
import { leaseDepth, leaseTransfer } from "./leaseTransfer.ts";
import { multipartTransfer } from "./multipartTransfer.ts";
import { negotiatePartSize } from "./negotiatePartSize.ts";
import { streamTransfer } from "./streamTransfer.ts";
import type { TransferOptions } from "./TransferOptions.ts";
import type { TransferResult } from "./TransferResult.ts";

const DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024;

export interface TransferEntryInput {
  readonly source: IOProvider;
  readonly sourceKey: string;
  readonly sink: IOProvider;
  readonly destKey: string;
  readonly properties: EntryProperties;
  readonly options: TransferOptions;
  readonly parentOperationId?: string;
  /** `"move"` rejects unbounded sources and prefers `directMove`. */
  readonly type: "copy" | "move";
  /** Whether a non-direct move deletes the source entry afterwards. */
  readonly deleteSource: boolean;
}

/** The path description before the transfer strategy is known. */
export function basePath(source: IOProvider, sink: IOProvider, options: TransferOptions): string {
  if (options.path !== undefined) {
    return options.path;
  }
  const via = options.converter ? describeConverter(options.converter) : "";
  return `${source.kind} -> ${sink.kind}${via}`;
}

function checkPayloadType(readable: StreamHandle, options: TransferOptions): void {
  const payloadType = readable.payloadType ?? BYTES_PAYLOAD_TYPE;
  const accepted = options.writePayloadTypes ?? [BYTES_PAYLOAD_TYPE];
  if (!accepted.includes(payloadType)) {
    throw new PermanentIOError(
      `Source payload type "${payloadType}" is not accepted by the sink (sink accepts: ${accepted.join(", ")})`,
    );
  }
}

async function cancelStream(handle: StreamHandle): Promise<void> {
  await (handle.stream as ReadableStream<Item>).cancel().catch(() => {});
}

/**
 * Transfers one entry: a direct provider transfer when eligible, otherwise
 * multipart, the lease path or plain streaming, after checking the payload
 * type and (for a move) that the source is bounded.
 */
export async function transferEntry(input: TransferEntryInput): Promise<TransferResult> {
  const { source, sourceKey, sink, destKey, properties, options, type } = input;
  throwIfAborted(options.signal);
  const path = basePath(source, sink, options);
  if (options.stop?.aborted) {
    return { stopped: true, bytes: 0, items: 0, path };
  }
  const operationId = crypto.randomUUID();
  const hooks = withParentOperationId(options.telemetry, input.parentOperationId);
  const context: EntryContext = { operationId, hooks, options, type };

  if (options.directTransfer !== false && source.canDirectTransfer?.(sink)) {
    const direct = type === "move" ? source.directMove : source.directCopy;
    if (direct) {
      await direct.call(source, sourceKey, destKey, { operationId, hooks });
      return { stopped: false, bytes: properties.size ?? 0, items: 0, path: `${path}, direct` };
    }
  }
  if (input.deleteSource && typeof source.delete !== "function") {
    throw new PermanentIOError("The source provider does not support delete, so it cannot move");
  }

  const retry = options.retry ?? DEFAULT_RETRY;
  let readable = await withRetry(() => source.getReadableStream(sourceKey), retry);
  try {
    checkPayloadType(readable, options);
    if (type === "move" && readable.bounded === false) {
      throw new PermanentIOError("cannot move a live source");
    }
  } catch (error) {
    await cancelStream(readable);
    throw error;
  }

  let outcome: EntryOutcome;
  let strategy: string;
  const size = properties.size;
  const threshold = options.multipartThreshold ?? DEFAULT_MULTIPART_THRESHOLD;
  const multipartWriter =
    readable.bounded !== false &&
    size !== undefined &&
    size >= threshold &&
    isRangeReadable(readable) &&
    typeof sink.getMultipartWriter === "function"
      ? sink.getMultipartWriter.bind(sink)
      : undefined;
  const partSize =
    multipartWriter && size !== undefined ? negotiatePartSize(source, sink, size) : undefined;

  if (
    multipartWriter &&
    partSize !== undefined &&
    size !== undefined &&
    isRangeReadable(readable)
  ) {
    await cancelStream(readable);
    const rangeReadable = readable;
    let writer: MultipartWriter | undefined;
    outcome = await withRetry(() => {
      // A retry continues the failed writer's upload when it has a resume token.
      const token = writer && isResumableWritable(writer) ? writer.resumeToken() : undefined;
      writer = token
        ? multipartWriter(destKey, partSize, { resume: token })
        : multipartWriter(destKey, partSize);
      return multipartTransfer({
        readable: rangeReadable,
        sink,
        writer,
        totalBytes: size,
        partSize,
        startOffset: token?.offset,
        limiter: options.concurrencyLimiter ?? defaultConcurrencyLimiter,
        context,
      });
    }, retry);
    strategy = "multipart";
  } else {
    let writable = await withRetry(() => sink.getWritableStream(destKey), retry);
    if (
      !options.converter &&
      isFillReadable(readable) &&
      isBufferProvider(writable) &&
      readable.domains.includes(writable.domain)
    ) {
      const depth = leaseDepth(writable, options.leaseDepth);
      let attempt = 0;
      outcome = await withRetry(async () => {
        if (attempt > 0) {
          readable = await source.getReadableStream(sourceKey);
          writable = await sink.getWritableStream(destKey);
        }
        attempt += 1;
        if (!isFillReadable(readable) || !isBufferProvider(writable)) {
          throw new PermanentIOError("Reopened handles no longer support the lease path");
        }
        return leaseTransfer(readable, writable, depth, context);
      }, retry);
      strategy = `lease (depth ${depth})`;
    } else {
      outcome = await streamTransfer({
        source,
        sourceKey,
        readable,
        sink,
        destKey,
        writable,
        totalBytes: size,
        context,
      });
      strategy = "stream";
    }
  }

  if (input.deleteSource && !outcome.stopped) {
    await withRetry(
      () => (source.delete as (key: string) => Promise<void>).call(source, sourceKey),
      retry,
    );
  }
  return { ...outcome, path: `${path}, ${strategy}` };
}
