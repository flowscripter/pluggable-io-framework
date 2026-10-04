/** The error a transfer rejects with when cancelled through `TransferOptions.signal`. */
export function createAbortError(): DOMException {
  return new DOMException("The transfer was aborted", "AbortError");
}

export function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw createAbortError();
  }
}

/**
 * Resolves with `"aborted"` or `"stopped"` as soon as `signal` or `stop`
 * fires, and never otherwise. `dispose` removes the listeners.
 */
export function watchSignals(
  signal: AbortSignal | undefined,
  stop: AbortSignal | undefined,
): { readonly fired: Promise<"aborted" | "stopped">; dispose(): void } {
  const cleanups: (() => void)[] = [];
  const fired = new Promise<"aborted" | "stopped">((resolve) => {
    for (const [target, outcome] of [
      [signal, "aborted"],
      [stop, "stopped"],
    ] as const) {
      if (!target) continue;
      if (target.aborted) {
        resolve(outcome);
        continue;
      }
      const listener = () => resolve(outcome);
      target.addEventListener("abort", listener, { once: true });
      cleanups.push(() => target.removeEventListener("abort", listener));
    }
  });
  return {
    fired,
    dispose() {
      for (const cleanup of cleanups) cleanup();
    },
  };
}
