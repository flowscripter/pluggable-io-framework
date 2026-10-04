import type { TelemetryHooks } from "@flowscripter/pluggable-io-framework-api";
import type { TransferOptions } from "./TransferOptions.ts";

/** Per-entry state shared by the transfer strategies. */
export interface EntryContext {
  readonly operationId: string;
  readonly hooks: TelemetryHooks;
  readonly options: TransferOptions;
  readonly type: "copy" | "move";
}

/** Bytes and items moved by one strategy run, and whether `stop` ended it. */
export interface EntryOutcome {
  readonly bytes: number;
  readonly items: number;
  readonly stopped: boolean;
}
