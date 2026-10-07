import type { TransferResult } from "./TransferResult.ts";

/** Sums per-entry results; the aggregate is stopped if any entry was. */
export function aggregateResults(
  results: readonly TransferResult[],
  path: string,
  stopped: boolean,
): TransferResult {
  return {
    stopped: stopped || results.some((result) => result.stopped),
    bytes: results.reduce((total, result) => total + result.bytes, 0),
    items: results.reduce((total, result) => total + result.items, 0),
    path,
  };
}
