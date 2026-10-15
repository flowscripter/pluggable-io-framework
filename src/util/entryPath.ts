import { type IOProvider, PermanentIOError } from "@flowscripter/pluggable-io-framework-api";
import { joinKey } from "./keys.ts";

/** Throws unless `path` is a non-empty relative path of plain segments. */
export function assertSafeEntryPath(path: string): void {
  const reject = (reason: string): never => {
    throw new PermanentIOError(`Unsafe entry path ${JSON.stringify(path)}: ${reason}`);
  };
  if (path === "") reject("empty");
  if (path.includes("\0")) reject("contains NUL");
  if (path.includes("\\")) reject("contains backslash");
  if (path.startsWith("/")) reject("absolute");
  if (/^[A-Za-z]:/.test(path)) reject("drive-letter");
  if (path.split("/").some((segment) => segment === "." || segment === "..")) {
    reject("contains . or .. segment");
  }
}

function collapse(key: string): string[] {
  const out: string[] = [];
  for (const segment of key.replace(/\\/g, "/").split("/")) {
    if (segment === "" || segment === ".") continue;
    if (segment === ".." && out.length > 0 && out.at(-1) !== "..") out.pop();
    else out.push(segment);
  }
  return out;
}

/**
 * Validates an entry path from a source listing and joins it onto `rootKey`
 * using the sink's `joinKey`, then checks the result is strictly inside
 * `rootKey` independently of how the provider joins.
 */
export function joinEntryPath(sink: IOProvider, rootKey: string, entryPath: string): string {
  assertSafeEntryPath(entryPath);
  const joined = joinKey(sink, rootKey, entryPath);
  const root = collapse(rootKey);
  const target = collapse(joined);
  if (target.length <= root.length || root.some((segment, i) => target[i] !== segment)) {
    throw new PermanentIOError(
      `Unsafe entry path ${JSON.stringify(entryPath)}: escapes the destination`,
    );
  }
  return joined;
}
