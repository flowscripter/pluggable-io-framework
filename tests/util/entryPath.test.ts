import { describe, expect, test } from "bun:test";
import { assertSafeEntryPath, joinEntryPath } from "../../src/util/entryPath.ts";
import { makeMemoryProvider, makeStore } from "../fixtures/memoryProvider.ts";

const BAD_PATHS: [string, string][] = [
  ["empty", ""],
  ["absolute", "/etc/passwd"],
  ["drive-letter", "C:/x"],
  ["drive-letter relative", "C:x"],
  ["backslash", "a\\b"],
  ["NUL", "a\0b"],
  ["parent segment", "../x"],
  ["nested parent segment", "a/../../x"],
  ["dot segment", "a/./b"],
  ["lone dot", "."],
  ["lone parent", ".."],
];

describe("entryPath", () => {
  const sink = makeMemoryProvider(makeStore());

  test("accepts normal nested paths", () => {
    expect(joinEntryPath(sink, "out", "a.txt")).toBe("out/a.txt");
    expect(joinEntryPath(sink, "out/", "d/e/f.txt")).toBe("out/d/e/f.txt");
    expect(joinEntryPath(sink, "", "a..b/..c")).toBe("a..b/..c");
  });

  for (const [name, path] of BAD_PATHS) {
    test(`rejects ${name}`, () => {
      expect(() => assertSafeEntryPath(path)).toThrow("Unsafe entry path");
      expect(() => joinEntryPath(sink, "out", path)).toThrow("Unsafe entry path");
    });
  }

  test("rejects a join that escapes the root even when the provider's joinKey is lenient", () => {
    const escaping = { ...sink, joinKey: (_root: string, name: string) => `elsewhere/${name}` };
    expect(() => joinEntryPath(escaping, "out", "a.txt")).toThrow("escapes the destination");
    const upward = { ...sink, joinKey: () => "out/../x" };
    expect(() => joinEntryPath(upward, "out", "a.txt")).toThrow("escapes the destination");
  });
});
