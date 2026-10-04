import { describe, expect, test } from "bun:test";
import { PayloadKind } from "@flowscripter/pluggable-io-framework-api";
import { describeConverter, describeKind } from "../../src/util/describePath.ts";

describe("describePath", () => {
  test("describeKind adds the domain in brackets", () => {
    expect(describeKind(PayloadKind.Js)).toBe("js");
    expect(describeKind(PayloadKind.Native, "host")).toBe("native[host]");
  });

  test("describeConverter names both ends and the cost", () => {
    const convert = (item: never) => item;
    expect(
      describeConverter({
        from: { kind: PayloadKind.Native },
        to: { kind: PayloadKind.Js },
        cost: 0,
        convert,
      }),
    ).toBe(" via native->js (zero-copy)");
    expect(
      describeConverter({
        from: { kind: PayloadKind.Native, domain: "host" },
        to: { kind: PayloadKind.Native, domain: "gpu" },
        cost: 1,
        convert,
      }),
    ).toBe(" via native[host]->native[gpu] (copy)");
  });
});
