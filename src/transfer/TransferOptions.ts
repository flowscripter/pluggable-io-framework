import type {
  PayloadConverterExtension,
  TelemetryHooks,
} from "@flowscripter/pluggable-io-framework-api";
import type { ConcurrencyLimiter } from "../concurrency/ConcurrencyLimiter.ts";
import type { RetryOptions } from "../retry/RetryOptions.ts";

export interface TransferOptions {
  readonly telemetry?: TelemetryHooks;
  /** Minimum entry size (bytes) before multipart transfer is attempted over plain streaming. */
  readonly multipartThreshold?: number;
  /**
   * The payload converter chosen by negotiation, applied to every item.
   * Without one, the source and sink must share a payload kind. Set by the
   * options `createProvidersForTransfer` returns.
   */
  readonly converter?: PayloadConverterExtension;
  /**
   * The negotiated path description reported in {@link TransferResult.path}.
   * Set by the options `createProvidersForTransfer` returns; without it the
   * path is described from the provider kinds.
   */
  readonly path?: string;
  /**
   * Payload type IDs the sink's factory accepts, which an opened source
   * handle's `payloadType` must be one of. Providers don't carry their
   * factory's declarations, so `createProvidersForTransfer` returns them in
   * its options. Defaults to `["bytes"]`.
   */
  readonly writePayloadTypes?: readonly string[];
  /**
   * Bounds concurrent multipart parts and recursive/pattern entries.
   * Defaults to the shared `defaultConcurrencyLimiter` - so two
   * `copy()`/`move()` calls in the same process share one cap unless a
   * caller passes its own isolated instance.
   */
  readonly concurrencyLimiter?: ConcurrencyLimiter;
  /** Retry policy for the non-direct transfer path. Defaults to `{ maxRetries: 3 }`. */
  readonly retry?: RetryOptions;
  /** Cancels the transfer: the source is cancelled, the sink aborted, and the transfer rejects with an `AbortError`. */
  readonly signal?: AbortSignal;
  /** Ends the transfer gracefully: the source is cancelled, the sink closed normally, and the result reports `stopped`. */
  readonly stop?: AbortSignal;
  /** When `false`, `directCopy`/`directMove` are never used. Defaults to `true`. */
  readonly directTransfer?: boolean;
  /** Maximum leases outstanding on the lease path, capped by the sink's `maxOutstanding`. Defaults to 2. */
  readonly leaseDepth?: number;
}
