import type { LocationTarget } from "@flowscripter/pluggable-io-framework-api";

export function entry(key: string): LocationTarget {
  return { kind: "entry", key };
}

export function container(key: string): LocationTarget {
  return { kind: "container", key };
}

export function pattern(containerKey: string, glob: string): LocationTarget {
  return { kind: "pattern", containerKey, pattern: glob };
}
