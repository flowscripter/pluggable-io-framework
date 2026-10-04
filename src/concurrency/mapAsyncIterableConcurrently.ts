import { AsyncChannel } from "./AsyncChannel.ts";
import type { ConcurrencyLimiter } from "./ConcurrencyLimiter.ts";

/**
 * Pulls items from `source` and runs `fn` on each, bounded by `limiter`:
 * exactly `limiter.maxConcurrency` worker loops each pull-then-process one
 * item at a time through `limiter.run`, so at most that many items are ever
 * pulled/in-flight at once (important for sources like a multipart reader
 * where pulling an item eagerly opens a file/network handle). Results are
 * yielded in completion order, not source order. If `fn` throws, no new
 * items are pulled but already-in-flight ones are allowed to finish before
 * the error is rethrown from the returned iterable.
 */
export function mapAsyncIterableConcurrently<T, R>(
  source: AsyncIterable<T>,
  fn: (item: T) => Promise<R>,
  limiter: ConcurrencyLimiter,
): AsyncIterable<R> {
  const channel = new AsyncChannel<R>();
  const iterator = source[Symbol.asyncIterator]();
  let stopped = false;
  let firstError: unknown;
  let errorRecorded = false;

  function recordError(error: unknown): void {
    stopped = true;
    if (!errorRecorded) {
      errorRecorded = true;
      firstError = error;
    }
  }

  async function worker(): Promise<void> {
    for (;;) {
      if (stopped) return;
      let next: IteratorResult<T>;
      try {
        next = await iterator.next();
      } catch (error) {
        recordError(error);
        return;
      }
      if (next.done) return;
      const value = next.value;
      try {
        const result = await limiter.run(() => fn(value));
        channel.push(result);
      } catch (error) {
        recordError(error);
        return;
      }
    }
  }

  void (async () => {
    const workerCount = Math.max(1, limiter.maxConcurrency);
    await Promise.all(Array.from({ length: workerCount }, () => worker()));
    if (errorRecorded) {
      channel.fail(firstError);
    } else {
      channel.close();
    }
  })();

  return channel;
}
