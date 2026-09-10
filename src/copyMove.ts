import {
  adaptReadableStream,
  identityChunkConverter,
  type ChunkConverter,
  type ChunkRef,
  type IOProvider,
  type ItemProperties,
  type Part,
  type PartSizeConstraints,
  type TelemetryHooks,
} from "@flowscripter/pluggable-io-framework-api";
import { chunkLength } from "./chunkLength.ts";
import {
  type ConcurrencyLimiter,
  defaultConcurrencyLimiter,
  mapAsyncIterableConcurrently,
} from "./ConcurrencyLimiter.ts";
import { withRetry, type RetryOptions } from "./withRetry.ts";

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
  /**
   * Bounds concurrent multipart parts and recursive-copy/move entries.
   * Defaults to the shared {@link defaultConcurrencyLimiter} - so two
   * `copy()`/`move()` calls in the same process share one cap unless a
   * caller passes its own isolated instance.
   */
  readonly concurrencyLimiter?: ConcurrencyLimiter;
  /** Retry policy for the non-direct transfer path. Defaults to `{ maxRetries: 3 }`. */
  readonly retry?: RetryOptions;
}

const DEFAULT_MULTIPART_THRESHOLD = 64 * 1024 * 1024;
const DEFAULT_PART_SIZE = 8 * 1024 * 1024;
const DEFAULT_RETRY: RetryOptions = { maxRetries: 3 };

const UNCONSTRAINED_PART_SIZE: PartSizeConstraints = {
  minPartSize: 0,
  maxPartSize: Infinity,
  maxParts: Infinity,
  defaultPartSize: DEFAULT_PART_SIZE,
};

/** Wraps `hooks` so any `onProgress` event passing through gets `parentOperationId` filled in, unless the emitter already set one (nested hierarchy). */
function withParentOperationId(
  hooks: TelemetryHooks | undefined,
  parentOperationId: string | undefined,
): TelemetryHooks {
  if (!hooks || parentOperationId === undefined) {
    return hooks ?? {};
  }
  return {
    ...hooks,
    onProgress: hooks.onProgress
      ? (event) => hooks.onProgress?.({ parentOperationId, ...event })
      : undefined,
  };
}

function joinPath(base: string, relative: string): string {
  if (relative === "") return base;
  if (base === "") return relative;
  return `${base.replace(/\/+$/, "")}/${relative}`;
}

/**
 * Reconciles `source`'s and `sink`'s {@link PartSizeConstraints} for a
 * transfer of `totalSize` bytes into a single part size satisfying both.
 * Returns `undefined` when the bounds are mutually infeasible (source's
 * minimum exceeds sink's maximum) - the caller should fall back to plain
 * streaming rather than throwing.
 */
function negotiatePartSize(
  source: IOProvider,
  sink: IOProvider,
  totalSize: number,
): number | undefined {
  const sourceConstraints = source.getPartSizeConstraints?.(totalSize) ?? UNCONSTRAINED_PART_SIZE;
  const sinkConstraints = sink.getPartSizeConstraints?.(totalSize) ?? UNCONSTRAINED_PART_SIZE;
  const minPartSize = Math.max(sourceConstraints.minPartSize, sinkConstraints.minPartSize);
  const maxPartSize = Math.min(sourceConstraints.maxPartSize, sinkConstraints.maxPartSize);
  const maxParts = Math.min(sourceConstraints.maxParts, sinkConstraints.maxParts);
  if (minPartSize > maxPartSize) {
    return undefined;
  }
  const byMaxParts = Math.ceil(totalSize / maxParts);
  const candidate = Math.max(
    minPartSize,
    sourceConstraints.defaultPartSize,
    sinkConstraints.defaultPartSize,
    byMaxParts,
  );
  return Math.min(candidate, maxPartSize);
}

function canUseMultipart(source: IOProvider, sink: IOProvider): boolean {
  return (
    typeof source.getMultipartReader === "function" && typeof sink.getMultipartWriter === "function"
  );
}

async function streamingTransfer(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  operationId: string,
  parentOperationId: string | undefined,
  options: TransferOptions,
  totalBytes: number | undefined,
): Promise<void> {
  const hooks = withParentOperationId(options.telemetry, parentOperationId);
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
      hooks.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
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
  parentOperationId: string | undefined,
  partSize: number,
  limiter: ConcurrencyLimiter,
  options: TransferOptions,
  totalBytes: number | undefined,
): Promise<void> {
  const hooks = withParentOperationId(options.telemetry, parentOperationId);
  let bytesProcessed = 0;

  async function transferPart(sourcePart: Part): Promise<Part> {
    const partOperationId = crypto.randomUUID();
    // Kind mismatch decided once per part, not once per chunk within it.
    const adaptedStream = adaptReadableStream(
      sourcePart.stream as ReadableStream<ChunkRef>,
      sourcePart.kind,
      sink.kind,
      options.chunkConverter ?? identityChunkConverter,
    );
    const reader = adaptedStream.getReader();
    const chunks: ChunkRef[] = [];
    let partBytesProcessed = 0;
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      const length = chunkLength(value);
      partBytesProcessed += length;
      bytesProcessed += length;
      hooks.onProgress?.({
        operationId: partOperationId,
        parentOperationId: operationId,
        type: "copy",
        bytesProcessed: partBytesProcessed,
      });
      hooks.onProgress?.({ operationId, type: "copy", bytesProcessed, totalBytes });
    }
    return {
      index: sourcePart.index,
      offset: sourcePart.offset,
      kind: sink.kind,
      stream: new ReadableStream<ChunkRef>({
        start(controller) {
          for (const chunk of chunks) controller.enqueue(chunk);
          controller.close();
        },
      }),
      complete: () => sourcePart.complete(),
    };
  }

  const writer = sink.getMultipartWriter(destPath, partSize);
  const transferredParts = mapAsyncIterableConcurrently(
    source.getMultipartReader(sourcePath, partSize),
    transferPart,
    limiter,
  );
  await writer.write(transferredParts);
}

async function copySingle(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions,
  parentOperationId: string | undefined,
  properties: ItemProperties,
): Promise<void> {
  const operationId = crypto.randomUUID();

  if (source.canDirectTransfer?.(sink) && source.directCopy) {
    await source.directCopy(sourcePath, destPath, {
      operationId,
      hooks: withParentOperationId(options.telemetry, parentOperationId),
    });
    return;
  }

  const threshold = options.multipartThreshold ?? DEFAULT_MULTIPART_THRESHOLD;
  const size = properties.size;
  const partSize =
    size !== undefined && size >= threshold && canUseMultipart(source, sink)
      ? negotiatePartSize(source, sink, size)
      : undefined;
  const limiter = options.concurrencyLimiter ?? defaultConcurrencyLimiter;
  const retryOptions = options.retry ?? DEFAULT_RETRY;

  await withRetry(async () => {
    if (partSize !== undefined) {
      await multipartTransfer(
        source,
        sourcePath,
        sink,
        destPath,
        operationId,
        parentOperationId,
        partSize,
        limiter,
        options,
        size,
      );
    } else {
      await streamingTransfer(
        source,
        sourcePath,
        sink,
        destPath,
        operationId,
        parentOperationId,
        options,
        size,
      );
    }
  }, retryOptions);
}

async function moveSingle(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions,
  parentOperationId: string | undefined,
  properties: ItemProperties,
): Promise<void> {
  if (source.canDirectTransfer?.(sink) && source.directMove) {
    const operationId = crypto.randomUUID();
    await source.directMove(sourcePath, destPath, {
      operationId,
      hooks: withParentOperationId(options.telemetry, parentOperationId),
    });
    return;
  }
  await copySingle(source, sourcePath, sink, destPath, options, parentOperationId, properties);
  await withRetry(() => source.delete(sourcePath), options.retry ?? DEFAULT_RETRY);
}

/**
 * Resolves the effective destination root for a recursive transfer,
 * following `cp -r`/`mv` semantics: a destination that doesn't exist becomes
 * the copy itself; an existing folder gets the source nested inside it as
 * `dest/<source-basename>`; an existing file is rejected.
 */
async function resolveRecursiveDestRoot(
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
): Promise<string> {
  let destProperties: ItemProperties | undefined;
  try {
    destProperties = await sink.getProperties(destPath);
  } catch {
    destProperties = undefined;
  }
  if (destProperties === undefined) {
    return destPath;
  }
  if (!destProperties.isFolder) {
    throw new Error(`Cannot copy/move folder "${sourcePath}" onto existing file "${destPath}"`);
  }
  const baseName = sourcePath.replace(/\/+$/, "").split("/").pop() || sourcePath;
  return joinPath(destPath, baseName);
}

async function recursiveTransfer(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions,
  mode: "copy" | "move",
): Promise<void> {
  if (source.canDirectTransfer?.(sink) && source.supportsRecursiveDirectTransfer) {
    const directFn = mode === "copy" ? source.directCopy : source.directMove;
    if (directFn) {
      const operationId = crypto.randomUUID();
      await directFn(sourcePath, destPath, { operationId, hooks: options.telemetry ?? {} });
      return;
    }
  }

  const resolvedDestRoot = await resolveRecursiveDestRoot(sourcePath, sink, destPath);
  const recursiveOperationId = crypto.randomUUID();
  const limiter = options.concurrencyLimiter ?? defaultConcurrencyLimiter;

  let discovered = 0;
  let completed = 0;
  const reportProgress = (): void => {
    options.telemetry?.onProgress?.({
      operationId: recursiveOperationId,
      type: mode,
      bytesProcessed: 0,
      itemsProcessed: completed,
      totalItems: discovered,
    });
  };

  async function processEntry(entry: { path: string; properties: ItemProperties }): Promise<void> {
    discovered += 1;
    reportProgress();
    const entrySourcePath = joinPath(sourcePath, entry.path);
    const entryDestPath = joinPath(resolvedDestRoot, entry.path);
    if (entry.properties.isFolder) {
      await sink.createFolder?.(entryDestPath);
    } else if (mode === "copy") {
      await copySingle(
        source,
        entrySourcePath,
        sink,
        entryDestPath,
        options,
        recursiveOperationId,
        entry.properties,
      );
    } else if (source.canDirectTransfer?.(sink) && source.directMove) {
      // A per-file directMove deletes its source atomically as part of the
      // provider's own operation - safe to do immediately. The non-direct
      // case below must NOT delete per entry: only after every entry
      // succeeds does the caller delete the whole source root once.
      const operationId = crypto.randomUUID();
      await source.directMove(entrySourcePath, entryDestPath, {
        operationId,
        hooks: withParentOperationId(options.telemetry, recursiveOperationId),
      });
    } else {
      await copySingle(
        source,
        entrySourcePath,
        sink,
        entryDestPath,
        options,
        recursiveOperationId,
        entry.properties,
      );
    }
    completed += 1;
    reportProgress();
  }

  for await (const _entry of mapAsyncIterableConcurrently(
    source.list(sourcePath, { recursive: true }),
    processEntry,
    limiter,
  )) {
    // work happens inside processEntry - nothing to do with the (void) result.
  }

  if (mode === "move") {
    await withRetry(() => source.delete(sourcePath), options.retry ?? DEFAULT_RETRY);
  }
}

/**
 * Copies `sourcePath` on `source` to `destPath` on `sink`. When `sourcePath`
 * is a folder, recurses: uses a folder-aware `directCopy` when the source
 * declares `supportsRecursiveDirectTransfer`, otherwise lists the folder and
 * transfers each entry (bounded by `options.concurrencyLimiter`), following
 * `cp -r` target semantics (see {@link resolveRecursiveDestRoot}).
 *
 * For a single file: uses `source.directCopy` when
 * `source.canDirectTransfer?.(sink)` reports eligibility. Otherwise falls
 * back to multipart transfer (when both sides support it, size crosses
 * `options.multipartThreshold`, and part-size negotiation succeeds) or plain
 * streaming - retried per `options.retry` on `TransientIOError`.
 */
export async function copy(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions = {},
): Promise<void> {
  const properties = await source.getProperties(sourcePath);
  if (properties.isFolder) {
    await recursiveTransfer(source, sourcePath, sink, destPath, options, "copy");
    return;
  }
  await copySingle(source, sourcePath, sink, destPath, options, undefined, properties);
}

/**
 * Moves `sourcePath` on `source` to `destPath` on `sink`. Folder handling
 * mirrors {@link copy}. For a single file, uses `source.directMove` when
 * eligible, otherwise performs a {@link copy} followed by deleting the
 * source item (each retried independently per `options.retry`).
 */
export async function move(
  source: IOProvider,
  sourcePath: string,
  sink: IOProvider,
  destPath: string,
  options: TransferOptions = {},
): Promise<void> {
  const properties = await source.getProperties(sourcePath);
  if (properties.isFolder) {
    await recursiveTransfer(source, sourcePath, sink, destPath, options, "move");
    return;
  }
  await moveSingle(source, sourcePath, sink, destPath, options, undefined, properties);
}
