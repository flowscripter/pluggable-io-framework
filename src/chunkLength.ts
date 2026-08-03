import { ChunkKind, type ChunkRef } from "@flowscripter/pluggable-io-framework-api";

export function chunkLength(chunk: ChunkRef): number {
  return chunk.kind === ChunkKind.Js ? chunk.data.byteLength : chunk.length;
}
