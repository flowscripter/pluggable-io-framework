import {
  type IOProvider,
  type LocationTarget,
  PermanentIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { baseName, joinKey } from "../util/keys.ts";
import { patternTransfer } from "./patternTransfer.ts";
import { recursiveTransfer } from "./recursiveTransfer.ts";
import { transferEntry } from "./transferEntry.ts";
import type { TransferOptions } from "./TransferOptions.ts";
import type { TransferResult } from "./TransferResult.ts";

async function checkSourceVariant(
  source: IOProvider,
  key: string,
  expectContainer: boolean,
  variant: string,
) {
  const properties = await source.getProperties(key);
  if (properties.isContainer !== expectContainer) {
    const actual = properties.isContainer ? "a container" : "an entry";
    throw new PermanentIOError(`Source "${key}" is declared as ${variant} but is ${actual}`);
  }
  return properties;
}

async function transfer(
  source: IOProvider,
  sourceTarget: LocationTarget,
  sink: IOProvider,
  destTarget: LocationTarget,
  options: TransferOptions,
  type: "copy" | "move",
): Promise<TransferResult> {
  if (destTarget.kind === "pattern") {
    throw new PermanentIOError("A destination cannot be a pattern");
  }
  if (sourceTarget.kind === "entry") {
    const properties = await checkSourceVariant(source, sourceTarget.key, false, "an entry");
    const destKey =
      destTarget.kind === "entry"
        ? destTarget.key
        : joinKey(sink, destTarget.key, baseName(sourceTarget.key));
    return transferEntry({
      source,
      sourceKey: sourceTarget.key,
      sink,
      destKey,
      properties,
      options,
      type,
      deleteSource: type === "move",
    });
  }
  if (destTarget.kind === "entry") {
    throw new PermanentIOError(
      sourceTarget.kind === "container"
        ? "Cannot transfer a container onto an entry"
        : "Cannot transfer a pattern of entries onto a single entry",
    );
  }
  if (sourceTarget.kind === "container") {
    await checkSourceVariant(source, sourceTarget.key, true, "a container");
    return recursiveTransfer(source, sourceTarget.key, sink, destTarget.key, options, type);
  }
  await checkSourceVariant(source, sourceTarget.containerKey, true, "a pattern container");
  return patternTransfer(
    source,
    sourceTarget.containerKey,
    sourceTarget.pattern,
    sink,
    destTarget.key,
    options,
    type,
  );
}

/**
 * Copies `sourceTarget` on `source` to `destTarget` on `sink`:
 * - entry -> entry writes exactly the destination key;
 * - entry -> container writes `<container>/<source basename>`;
 * - container -> container copies recursively with `cp -r` semantics;
 * - pattern -> container copies each matching entry;
 * - container or pattern -> entry, and any pattern destination, are rejected.
 *
 * Each entry uses `directCopy` when eligible (and `options.directTransfer`
 * is not `false`), otherwise multipart, the lease path or plain streaming.
 */
export function copy(
  source: IOProvider,
  sourceTarget: LocationTarget,
  sink: IOProvider,
  destTarget: LocationTarget,
  options: TransferOptions = {},
): Promise<TransferResult> {
  return transfer(source, sourceTarget, sink, destTarget, options, "copy");
}

/**
 * Moves `sourceTarget` on `source` to `destTarget` on `sink`, with the same
 * target rules as {@link copy}. Non-direct moves copy and then delete the
 * source. Unbounded sources are rejected, and a stopped move never deletes
 * its source.
 */
export function move(
  source: IOProvider,
  sourceTarget: LocationTarget,
  sink: IOProvider,
  destTarget: LocationTarget,
  options: TransferOptions = {},
): Promise<TransferResult> {
  return transfer(source, sourceTarget, sink, destTarget, options, "move");
}
