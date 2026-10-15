import type { IOProvider } from "@flowscripter/pluggable-io-framework-api";

/** Joins with `/` - the default when a provider has no `joinKey`. */
export function defaultJoinKey(containerKey: string, name: string): string {
  if (name === "") return containerKey;
  if (containerKey === "") return name;
  let end = containerKey.length;
  while (end > 0 && containerKey.charCodeAt(end - 1) === 47) end--;
  return `${containerKey.slice(0, end)}/${name}`;
}

/** Joins a child name onto a container key using the provider's own rule. */
export function joinKey(provider: IOProvider, containerKey: string, name: string): string {
  return provider.joinKey
    ? provider.joinKey(containerKey, name)
    : defaultJoinKey(containerKey, name);
}

/** The last segment of a key, treating both `/` and `\` as separators. */
export function baseName(key: string): string {
  const segments = key.split(/[\\/]+/).filter((segment) => segment !== "");
  return segments.at(-1) ?? key;
}
