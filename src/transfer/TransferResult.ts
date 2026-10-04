export interface TransferResult {
  /** Whether `stop` ended the transfer early (output is truncated for a bounded source). */
  readonly stopped: boolean;
  readonly bytes: number;
  readonly items: number;
  /** Human-readable description of the negotiated transfer path. */
  readonly path: string;
}
