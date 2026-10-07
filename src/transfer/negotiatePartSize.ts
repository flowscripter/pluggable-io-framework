import type { IOProvider, PartSizeConstraints } from "@flowscripter/pluggable-io-framework-api";

const DEFAULT_PART_SIZE = 8 * 1024 * 1024;

const UNCONSTRAINED_PART_SIZE: PartSizeConstraints = {
  minPartSize: 0,
  maxPartSize: Infinity,
  maxParts: Infinity,
  defaultPartSize: DEFAULT_PART_SIZE,
};

/**
 * Reconciles `source`'s and `sink`'s {@link PartSizeConstraints} for a
 * transfer of `totalSize` bytes into a single part size satisfying both.
 * Returns `undefined` when the bounds are mutually infeasible (source's
 * minimum exceeds sink's maximum) - the caller should fall back to plain
 * streaming rather than throwing.
 */
export function negotiatePartSize(
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
