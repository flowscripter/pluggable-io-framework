import type { TelemetryHooks } from "@flowscripter/pluggable-io-framework-api";

/** Wraps `hooks` so any `onProgress` event passing through gets `parentOperationId` filled in, unless the emitter already set one (nested hierarchy). */
export function withParentOperationId(
  hooks: TelemetryHooks | undefined,
  parentOperationId: string | undefined,
): TelemetryHooks {
  if (!hooks || parentOperationId === undefined) {
    return hooks ?? {};
  }
  return {
    ...hooks,
    onProgress: hooks.onProgress
      ? (event) => hooks.onProgress?.({ parentOperationId, ...event })
      : undefined,
  };
}
