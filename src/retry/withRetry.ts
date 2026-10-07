import { TransientIOError } from "@flowscripter/pluggable-io-framework-api";
import { backoff, type RetryOptions } from "./RetryOptions.ts";

/**
 * Runs `fn`, retrying only on `TransientIOError` up to `options.maxRetries`
 * times with a delay between attempts. Any other error - including a plain
 * `Error` a provider hasn't wrapped - is treated as non-retryable and
 * rethrown immediately.
 */
export async function withRetry<T>(fn: () => Promise<T>, options: RetryOptions): Promise<T> {
  let attempt = 0;
  for (;;) {
    try {
      return await fn();
    } catch (error) {
      if (!(error instanceof TransientIOError) || attempt >= options.maxRetries) {
        throw error;
      }
      attempt += 1;
      await backoff(options, attempt);
    }
  }
}
