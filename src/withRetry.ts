import { TransientIOError } from "@flowscripter/pluggable-io-framework-api";

export interface RetryOptions {
  readonly maxRetries: number;
  /** Delay in ms before the given retry attempt (1-based). Defaults to `min(200 * 2^attempt, 5000)`. */
  readonly backoffMs?: (attempt: number) => number;
}

function defaultBackoffMs(attempt: number): number {
  return Math.min(200 * 2 ** attempt, 5000);
}

/**
 * Runs `fn`, retrying only on `TransientIOError` up to `options.maxRetries`
 * times with a delay between attempts. Any other error - including a plain
 * `Error` a provider hasn't wrapped - is treated as non-retryable and
 * rethrown immediately.
 */
export async function withRetry<T>(fn: () => Promise<T>, options: RetryOptions): Promise<T> {
  const backoffMs = options.backoffMs ?? defaultBackoffMs;
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (error) {
      if (!(error instanceof TransientIOError) || attempt >= options.maxRetries) {
        throw error;
      }
      attempt += 1;
      await new Promise((resolve) => setTimeout(resolve, backoffMs(attempt)));
    }
  }
}
