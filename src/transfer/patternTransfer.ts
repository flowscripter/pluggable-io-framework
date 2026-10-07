import {
  type EntryProperties,
  type IOProvider,
  PermanentIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { defaultConcurrencyLimiter } from "../concurrency/ConcurrencyLimiter.ts";
import { mapAsyncIterableConcurrently } from "../concurrency/mapAsyncIterableConcurrently.ts";
import { throwIfAborted } from "../util/abort.ts";
import { globToRegex } from "../util/globToRegex.ts";
import { joinKey } from "../util/keys.ts";
import { aggregateResults } from "./aggregateResults.ts";
import { basePath, transferEntry } from "./transferEntry.ts";
import type { TransferOptions } from "./TransferOptions.ts";
import type { TransferResult } from "./TransferResult.ts";

/**
 * Transfers every entry directly inside `containerKey` whose name matches
 * the glob `pattern` into the destination container, one single-entry
 * transfer per match, bounded by the concurrency limiter. Matching
 * containers are skipped. A move deletes each source entry after it is
 * transferred.
 */
export async function patternTransfer(
  source: IOProvider,
  containerKey: string,
  pattern: string,
  sink: IOProvider,
  destContainerKey: string,
  options: TransferOptions,
  type: "copy" | "move",
): Promise<TransferResult> {
  if (typeof source.list !== "function") {
    throw new PermanentIOError("The source provider does not support listing containers");
  }
  const operationId = crypto.randomUUID();
  const limiter = options.concurrencyLimiter ?? defaultConcurrencyLimiter;
  let discovered = 0;
  let completed = 0;
  const reportProgress = (): void => {
    options.telemetry?.onProgress?.({
      operationId,
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
    if (options.stop?.aborted || entry.properties.isContainer) {
      return undefined;
    }
    discovered += 1;
    reportProgress();
    const result = await transferEntry({
      source,
      sourceKey: joinKey(source, containerKey, entry.path),
      sink,
      destKey: joinKey(sink, destContainerKey, entry.path),
      properties: entry.properties,
      options,
      parentOperationId: operationId,
      type,
      deleteSource: type === "move",
    });
    completed += 1;
    reportProgress();
    return result;
  }

  const results: TransferResult[] = [];
  for await (const result of mapAsyncIterableConcurrently(
    source.list(containerKey, { recursive: false, regex: globToRegex(pattern) }),
    processEntry,
    limiter,
  )) {
    if (result) results.push(result);
  }
  return aggregateResults(results, basePath(source, sink, options), options.stop?.aborted ?? false);
}
