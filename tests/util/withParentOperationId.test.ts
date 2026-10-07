import { describe, expect, test } from "bun:test";
import { withParentOperationId } from "../../src/util/withParentOperationId.ts";

describe("withParentOperationId", () => {
  test("returns the hooks unchanged without a parent id, and {} without hooks", () => {
    const hooks = { onProgress: () => {} };
    expect(withParentOperationId(hooks, undefined)).toBe(hooks);
    expect(withParentOperationId(undefined, "p")).toEqual({});
  });

  test("fills in parentOperationId unless the event already has one", () => {
    const events: { parentOperationId?: string }[] = [];
    const wrapped = withParentOperationId({ onProgress: (event) => events.push(event) }, "p");
    wrapped.onProgress?.({ operationId: "a", type: "copy", bytesProcessed: 0 });
    wrapped.onProgress?.({
      operationId: "b",
      parentOperationId: "own",
      type: "copy",
      bytesProcessed: 0,
    });
    expect(events.map((e) => e.parentOperationId)).toEqual(["p", "own"]);
    expect(withParentOperationId({}, "p").onProgress).toBeUndefined();
  });
});
