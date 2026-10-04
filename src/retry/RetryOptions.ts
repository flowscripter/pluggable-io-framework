export interface RetryOptions {
  /**
   * Maximum retries after a `TransientIOError`. For an unbounded source this
   * counts consecutive failures and resets once an item arrives.
   */
  readonly maxRetries: number;
  /** Delay in ms before the given retry attempt (1-based). Defaults to `min(200 * 2^attempt, 5000)`. */
  readonly backoffMs?: (attempt: number) => number;
  /**
   * Unbounded sources only: what to do when the source fails and has to be
   * reconnected, leaving a gap. Defaults to `"continue"`.
   */
  readonly onGap?: "continue" | "fail";
}

export const DEFAULT_RETRY: RetryOptions = { maxRetries: 3 };

export function defaultBackoffMs(attempt: number): number {
  return Math.min(200 * 2 ** attempt, 5000);
}

/** Waits for the backoff delay before retry `attempt`. */
export function backoff(options: RetryOptions, attempt: number): Promise<void> {
  const delay = (options.backoffMs ?? defaultBackoffMs)(attempt);
  return new Promise((resolve) => setTimeout(resolve, delay));
}
