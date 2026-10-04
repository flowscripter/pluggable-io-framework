import { describe, expect, test } from "bun:test";
import { aggregateResults } from "../../src/transfer/aggregateResults.ts";

describe("aggregateResults", () => {
  test("sums bytes and items and keeps the given path", () => {
    const result = aggregateResults(
      [
        { stopped: false, bytes: 2, items: 1, path: "a" },
        { stopped: false, bytes: 3, items: 2, path: "b" },
      ],
      "base",
      false,
    );
    expect(result).toEqual({ stopped: false, bytes: 5, items: 3, path: "base" });
  });

  test("is stopped if requested or if any entry stopped", () => {
    expect(aggregateResults([], "p", true).stopped).toBe(true);
    expect(
      aggregateResults([{ stopped: true, bytes: 0, items: 0, path: "p" }], "p", false).stopped,
    ).toBe(true);
  });
});
