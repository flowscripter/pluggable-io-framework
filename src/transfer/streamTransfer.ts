import {
  type IOProvider,
  type Item,
  isRangeReadable,
  isResumableWritable,
  type StreamHandle,
  TransientIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { backoff, DEFAULT_RETRY } from "../retry/RetryOptions.ts";
import { createAbortError, watchSignals } from "../util/abort.ts";
import { applyPayloadConverter } from "../util/applyPayloadConverter.ts";
import { itemLength } from "../util/itemLength.ts";
import type { EntryContext, EntryOutcome } from "./EntryContext.ts";

/** Which side of the transfer a failure came from. */
class SideError extends Error {
  public constructor(
    public readonly side: "source" | "sink",
    cause: unknown,
  ) {
    super("transfer side failed", { cause });
  }
}

export interface StreamTransferInput {
  readonly source: IOProvider;
  readonly sourceKey: string;
  readonly readable: StreamHandle;
  readonly sink: IOProvider;
  readonly destKey: string;
  readonly writable: StreamHandle;
  /** Total bytes for a bounded source, when known. */
  readonly totalBytes?: number;
  readonly context: EntryContext;
}

function ignore(): void {}

/**
 * Pipes a readable handle into a writable handle item by item, honouring
 * `signal` (abort) and `stop` (graceful close).
 *
 * On a `TransientIOError`:
 * - bounded source with `RangeReadable`, and a writable exposing a resume
 *   token: the sink is reopened with the token and the source re-read from
 *   the sink's `startOffset`;
 * - other bounded transfers restart from scratch;
 * - unbounded source failure: the source is reopened, the sink kept open,
 *   the first new item flagged with `discontinuity`, and `onGap` reported;
 * - unbounded sink failure: the sink is reopened with its resume token, or
 *   the transfer fails.
 *
 * `retry.maxRetries` counts consecutive failures and resets once an item is
 * written.
 */
export async function streamTransfer(input: StreamTransferInput): Promise<EntryOutcome> {
  const { source, sourceKey, sink, destKey, totalBytes, context } = input;
  const { options, hooks, operationId, type } = context;
  const retry = options.retry ?? DEFAULT_RETRY;
  const bounded = input.readable.bounded !== false;
  const watch = watchSignals(options.signal, options.stop);

  let readable = input.readable;
  let writable = input.writable;
  const convert = (stream: ReadableStream<Item>) =>
    applyPayloadConverter(stream, readable.kind, sink.kind, options.converter);
  let reader = convert(readable.stream as ReadableStream<Item>).getReader();
  let writer = (writable.stream as WritableStream<Item>).getWriter();
  let bytes = 0;
  let items = 0;
  let failures = 0;
  let markGap = false;
  let recover: { side: "source" | "sink"; run: () => Promise<void> } | undefined;

  async function pump(): Promise<"done" | "stopped"> {
    for (;;) {
      let read:
        | Awaited<ReturnType<ReadableStreamDefaultReader<Item>["read"]>>
        | "aborted"
        | "stopped";
      try {
        read = await Promise.race([reader.read(), watch.fired]);
      } catch (error) {
        throw new SideError("source", error);
      }
      if (read === "aborted") throw createAbortError();
      if (read === "stopped") return "stopped";
      if (read.done) return "done";

      let item = read.value;
      if (markGap) {
        item = { ...item, attributes: { ...item.attributes, discontinuity: true } };
        markGap = false;
      }
      let written: void | "aborted" | "stopped";
      try {
        written = await Promise.race([writer.write(item), watch.fired]);
      } catch (error) {
        throw new SideError("sink", error);
      }
      if (written === "aborted") throw createAbortError();
      bytes += itemLength(item);
      items += 1;
      failures = 0;
      hooks.onProgress?.({
        operationId,
        type,
        bytesProcessed: bytes,
        totalBytes: bounded ? totalBytes : undefined,
      });
      if (written === "stopped") return "stopped";
    }
  }

  try {
    for (;;) {
      try {
        if (recover) {
          const action = recover;
          recover = undefined;
          try {
            await action.run();
          } catch (error) {
            throw new SideError(action.side, error);
          }
        }
        const outcome = await pump();
        if (outcome === "stopped") {
          await reader.cancel().catch(ignore);
        }
        await writer.close();
        return { bytes, items, stopped: outcome === "stopped" };
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") {
          await reader.cancel(error).catch(ignore);
          await writer.abort(error).catch(ignore);
          throw error;
        }
        const side = error instanceof SideError ? error.side : "sink";
        const cause = error instanceof SideError ? error.cause : error;
        failures += 1;
        const retryable = cause instanceof TransientIOError && failures <= retry.maxRetries;

        if (retryable && !bounded && side === "source" && retry.onGap !== "fail") {
          await reader.cancel().catch(ignore);
          const attempt = failures;
          recover = {
            side: "source",
            run: async () => {
              await backoff(retry, attempt);
              readable = await source.getReadableStream(sourceKey);
              reader = convert(readable.stream as ReadableStream<Item>).getReader();
              markGap = true;
              hooks.onGap?.(operationId, { atBytes: bytes, attempt });
            },
          };
          continue;
        }

        const token =
          retryable && isResumableWritable(writable) ? writable.resumeToken() : undefined;
        const canResume = token !== undefined && (!bounded || isRangeReadable(readable));
        if (retryable && canResume) {
          // The failed sink is released, not aborted, so a provider that
          // discards partial output on abort keeps it for the resume.
          writer.releaseLock();
          if (bounded) {
            await reader.cancel().catch(ignore);
          }
          const attempt = failures;
          recover = {
            side: "sink",
            run: async () => {
              await backoff(retry, attempt);
              const resumed = await sink.getWritableStream(destKey, { resume: token });
              writable = resumed;
              writer = (resumed.stream as WritableStream<Item>).getWriter();
              if (bounded && isRangeReadable(readable)) {
                const startOffset = resumed.startOffset ?? token.offset;
                const end = totalBytes ?? Number.MAX_SAFE_INTEGER;
                reader = convert(
                  (await readable.readRange(startOffset, end)) as ReadableStream<Item>,
                ).getReader();
                bytes = startOffset;
              }
            },
          };
          continue;
        }

        await reader.cancel(cause).catch(ignore);
        await writer.abort(cause).catch(ignore);
        if (!retryable || !bounded) {
          throw cause;
        }
        const attempt = failures;
        recover = {
          side: "source",
          run: async () => {
            await backoff(retry, attempt);
            readable = await source.getReadableStream(sourceKey);
            reader = convert(readable.stream as ReadableStream<Item>).getReader();
            writable = await sink.getWritableStream(destKey);
            writer = (writable.stream as WritableStream<Item>).getWriter();
            bytes = 0;
            items = 0;
          },
        };
      }
    }
  } finally {
    watch.dispose();
  }
}
