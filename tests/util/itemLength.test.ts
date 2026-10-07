import { describe, expect, test } from "bun:test";
import { PayloadKind } from "@flowscripter/pluggable-io-framework-api";
import { itemLength } from "../../src/util/itemLength.ts";

describe("itemLength", () => {
  test("measures js and native payloads", () => {
    expect(itemLength({ payload: { kind: PayloadKind.Js, data: new Uint8Array(3) } })).toBe(3);
    expect(
      itemLength({
        payload: { kind: PayloadKind.Native, domain: "host", ptr: 0, length: 7, release: () => {} },
      }),
    ).toBe(7);
  });
});
