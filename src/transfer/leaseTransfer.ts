import type {
  BufferLease,
  BufferProvider,
  FillReadable,
  Item,
  StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";
import { createAbortError } from "../util/abort.ts";
import type { EntryContext, EntryOutcome } from "./EntryContext.ts";

export const DEFAULT_LEASE_DEPTH = 2;

/** The number of leases the engine keeps outstanding for `writable`. */
export function leaseDepth(writable: BufferProvider, requested: number | undefined): number {
  return Math.max(
    1,
    Math.min(requested ?? DEFAULT_LEASE_DEPTH, writable.maxOutstanding ?? Infinity),
  );
}

function ignore(): void {}

/**
 * Moves data by having the source fill buffers handed out by the sink:
 * `acquire -> readInto -> commit`, with up to `depth` leases outstanding so
 * the next read overlaps earlier commits. `readInto` calls are serialised
 * and commits happen in acquisition order. Every lease not yet committed is
 * released on abort, error or end of stream.
 */
export async function leaseTransfer(
  readable: StreamHandle & FillReadable,
  writable: StreamHandle & BufferProvider,
  depth: number,
  context: EntryContext,
): Promise<EntryOutcome> {
  const { options, hooks, operationId, type } = context;
  const outstanding = new Set<BufferLease>();
  const pending: Promise<void>[] = [];
  let commitChain: Promise<void> = Promise.resolve();
  let bytes = 0;
  let items = 0;
  let stopped = false;
  const writer = (writable.stream as WritableStream<Item>).getWriter();

  try {
    for (;;) {
      if (options.signal?.aborted) throw createAbortError();
      if (options.stop?.aborted) {
        stopped = true;
        break;
      }
      while (pending.length >= depth) {
        await pending.shift();
      }
      const lease = await writable.acquire();
      outstanding.add(lease);
      const length = await readable.readInto(lease);
      if (length === null) {
        outstanding.delete(lease);
        lease.release();
        break;
      }
      bytes += length;
      items += 1;
      const committed = commitChain.then(async () => {
        await lease.commit(length);
        outstanding.delete(lease);
      });
      committed.catch(ignore);
      commitChain = committed;
      pending.push(committed);
      hooks.onProgress?.({
        operationId,
        type,
        bytesProcessed: bytes,
        totalBytes: undefined,
      });
    }
    await commitChain;
    await writer.close();
    if (!stopped) {
      await (readable.stream as ReadableStream<Item>).cancel().catch(ignore);
    }
    return { bytes, items, stopped };
  } catch (error) {
    for (const lease of outstanding) {
      lease.release();
    }
    await writer.abort(error).catch(ignore);
    await (readable.stream as ReadableStream<Item>).cancel(error).catch(ignore);
    throw error;
  }
}
