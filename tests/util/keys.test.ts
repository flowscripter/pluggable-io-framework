import { describe, expect, test } from "bun:test";
import { baseName, defaultJoinKey, joinKey } from "../../src/util/keys.ts";
import { makeMemoryProvider, makeStore } from "../fixtures/memoryProvider.ts";

describe("keys", () => {
  test("defaultJoinKey joins with / and handles empty parts", () => {
    expect(defaultJoinKey("a/", "b")).toBe("a/b");
    expect(defaultJoinKey("", "b")).toBe("b");
    expect(defaultJoinKey("a", "")).toBe("a");
  });

  test("joinKey prefers the provider's own rule", () => {
    const provider = makeMemoryProvider(makeStore());
    expect(joinKey(provider, "a", "b")).toBe("a/b");
    const custom = { ...provider, joinKey: (a: string, b: string) => `${a}\\${b}` };
    expect(joinKey(custom, "a", "b")).toBe("a\\b");
  });

  test("baseName takes the last segment for either separator", () => {
    expect(baseName("a/b/c.txt")).toBe("c.txt");
    expect(baseName("C:\\dir\\file.txt")).toBe("file.txt");
    expect(baseName("dir/")).toBe("dir");
    expect(baseName("/")).toBe("/");
  });
});
