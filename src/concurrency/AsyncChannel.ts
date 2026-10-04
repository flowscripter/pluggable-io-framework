/** Minimal single-consumer async FIFO queue. */
export class AsyncChannel<T> implements AsyncIterable<T> {
  #items: T[] = [];
  #waiters: { resolve: (result: IteratorResult<T>) => void; reject: (error: unknown) => void }[] =
    [];
  #closed = false;
  #hasError = false;
  #error: unknown;

  push(item: T): void {
    const waiter = this.#waiters.shift();
    if (waiter) {
      waiter.resolve({ value: item, done: false });
    } else {
      this.#items.push(item);
    }
  }

  close(): void {
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      if (this.#hasError) {
        waiter.reject(this.#error);
      } else {
        waiter.resolve({ value: undefined as never, done: true });
      }
    }
  }

  fail(error: unknown): void {
    this.#hasError = true;
    this.#error = error;
    this.close();
  }

  async #next(): Promise<IteratorResult<T>> {
    if (this.#items.length > 0) {
      return { value: this.#items.shift() as T, done: false };
    }
    if (this.#closed) {
      if (this.#hasError) {
        throw this.#error;
      }
      return { value: undefined as never, done: true };
    }
    return new Promise((resolve, reject) => this.#waiters.push({ resolve, reject }));
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return { next: () => this.#next() };
  }
}
