import {
  type EntryProperties,
  type IOProvider,
  PermanentIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { defaultConcurrencyLimiter } from "../concurrency/ConcurrencyLimiter.ts";
import { mapAsyncIterableConcurrently } from "../concurrency/mapAsyncIterableConcurrently.ts";
import { DEFAULT_RETRY } from "../retry/RetryOptions.ts";
import { withRetry } from "../retry/withRetry.ts";
import { throwIfAborted } from "../util/abort.ts";
import { baseName, joinKey } from "../util/keys.ts";
import { aggregateResults } from "./aggregateResults.ts";
import { basePath, transferEntry } from "./transferEntry.ts";
import type { TransferOptions } from "./TransferOptions.ts";
import type { TransferResult } from "./TransferResult.ts";

/**
 * Resolves the effective destination root for a recursive transfer,
 * following `cp -r`/`mv` semantics: a destination that doesn't exist becomes
 * the copy itself; an existing container gets the source nested inside it
 * as `dest/<source-basename>`; an existing entry is rejected.
 */
async function resolveRecursiveDestRoot(
  sourceKey: string,
  sink: IOProvider,
  destKey: string,
): Promise<string> {
  let destProperties: EntryProperties | undefined;
  try {
    destProperties = await sink.getProperties(destKey);
  } catch {
    destProperties = undefined;
  }
  if (destProperties === undefined) {
    return destKey;
  }
  if (!destProperties.isContainer) {
    throw new PermanentIOError(
      `Cannot copy/move container "${sourceKey}" onto existing entry "${destKey}"`,
    );
  }
  return joinKey(sink, destKey, baseName(sourceKey));
}

/**
 * Transfers a whole container. Uses a container-aware
 * `directCopy`/`directMove` when the source declares
 * `supportsRecursiveDirectTransfer`; otherwise lists the container and
 * transfers each entry, bounded by the concurrency limiter. A non-direct
 * move deletes the source container once, after every entry succeeded and
 * only if the transfer was not stopped.
 */
export async function recursiveTransfer(
  source: IOProvider,
  sourceKey: string,
  sink: IOProvider,
  destKey: string,
  options: TransferOptions,
  type: "copy" | "move",
): Promise<TransferResult> {
  const path = basePath(source, sink, options);
  const resolvedDestRoot = await resolveRecursiveDestRoot(sourceKey, sink, destKey);

  if (
    options.directTransfer !== false &&
    source.canDirectTransfer?.(sink) &&
    source.supportsRecursiveDirectTransfer
  ) {
    const direct = type === "move" ? source.directMove : source.directCopy;
    if (direct) {
      throwIfAborted(options.signal);
      const telemetry = { operationId: crypto.randomUUID(), hooks: options.telemetry ?? {} };
      await direct.call(source, sourceKey, resolvedDestRoot, telemetry);
      return { stopped: false, bytes: 0, items: 0, path: `${path}, direct` };
    }
  }
  if (typeof source.list !== "function") {
    throw new PermanentIOError("The source provider does not support listing containers");
  }
  if (type === "move" && typeof source.delete !== "function") {
    throw new PermanentIOError("The source provider does not support delete, so it cannot move");
  }

  const recursiveOperationId = crypto.randomUUID();
  const limiter = options.concurrencyLimiter ?? defaultConcurrencyLimiter;
  let discovered = 0;
  let completed = 0;
  const reportProgress = (): void => {
    options.telemetry?.onProgress?.({
      operationId: recursiveOperationId,
      type,
      bytesProcessed: 0,
      entriesProcessed: completed,
      totalEntries: discovered,
    });
  };

  async function processEntry(entry: {
    path: string;
    properties: EntryProperties;
  }): Promise<TransferResult | undefined> {
    throwIfAborted(options.signal);
    if (options.stop?.aborted) {
      return undefined;
    }
    discovered += 1;
    reportProgress();
    const entryDestKey = joinKey(sink, resolvedDestRoot, entry.path);
    let result: TransferResult | undefined;
    if (entry.properties.isContainer) {
      await sink.createContainer?.(entryDestKey);
    } else {
      result = await transferEntry({
        source,
        sourceKey: joinKey(source, sourceKey, entry.path),
        sink,
        destKey: entryDestKey,
        properties: entry.properties,
        options,
        parentOperationId: recursiveOperationId,
        type,
        deleteSource: false,
      });
    }
    completed += 1;
    reportProgress();
    return result;
  }

  const results: TransferResult[] = [];
  for await (const result of mapAsyncIterableConcurrently(
    source.list(sourceKey, { recursive: true }),
    processEntry,
    limiter,
  )) {
    if (result) results.push(result);
  }

  const outcome = aggregateResults(results, path, options.stop?.aborted ?? false);
  if (type === "move" && !outcome.stopped) {
    await withRetry(
      () => (source.delete as (key: string) => Promise<void>).call(source, sourceKey),
      options.retry ?? DEFAULT_RETRY,
    );
  }
  return outcome;
}
