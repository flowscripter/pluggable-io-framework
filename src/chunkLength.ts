import type { ChunkRef } from "@flowscripter/pluggable-io-framework-api";

export function chunkLength(chunk: ChunkRef): number {
  return chunk.kind === "js" ? chunk.data.byteLength : chunk.length;
}
