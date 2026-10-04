import { describe, expect, test } from "bun:test";
import { globToRegex } from "../../src/util/globToRegex.ts";

describe("globToRegex", () => {
  test("* and ? match within one name", () => {
    expect(globToRegex("*.txt").test("a.txt")).toBe(true);
    expect(globToRegex("*.txt").test("a.md")).toBe(false);
    expect(globToRegex("*.txt").test("dir/a.txt")).toBe(false);
    expect(globToRegex("?.txt").test("a.txt")).toBe(true);
    expect(globToRegex("?.txt").test("ab.txt")).toBe(false);
  });

  test("character classes, including negation", () => {
    expect(globToRegex("[ab].txt").test("a.txt")).toBe(true);
    expect(globToRegex("[ab].txt").test("c.txt")).toBe(false);
    expect(globToRegex("[!ab].txt").test("c.txt")).toBe(true);
    expect(globToRegex("[!ab].txt").test("a.txt")).toBe(false);
  });

  test("regex metacharacters and an unclosed [ match literally", () => {
    expect(globToRegex("a+(b).txt").test("a+(b).txt")).toBe(true);
    expect(globToRegex("a.txt").test("abtxt")).toBe(false);
    expect(globToRegex("[a").test("[a")).toBe(true);
  });
});
