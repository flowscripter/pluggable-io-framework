import { describe, expect, test } from "bun:test";
import { PayloadKind } from "@flowscripter/pluggable-io-framework-api";
import { endpointsOf, negotiateTransfer } from "../../src/registry/negotiateTransfer.ts";
import { makeConverter, makeFakeFactory } from "../fixtures/fakeFactories.ts";

const fileJs = makeFakeFactory({ protocol: "file" });
const fileNative = makeFakeFactory({
  protocol: "file",
  kind: PayloadKind.Native,
  domains: ["host", "gpu"],
});
const s3Js = makeFakeFactory({ protocol: "s3" });
const gpuOnly = makeFakeFactory({ protocol: "dev", kind: PayloadKind.Native, domains: ["gpu"] });

describe("endpointsOf", () => {
  test("js factories have one endpoint; native ones one per domain, defaulting to host", () => {
    expect(endpointsOf(fileJs).map((e) => e.domain)).toEqual([undefined]);
    expect(endpointsOf(fileNative).map((e) => e.domain)).toEqual(["host", "gpu"]);
    const noDomains = makeFakeFactory({ protocol: "x", kind: PayloadKind.Native });
    expect(endpointsOf(noDomains).map((e) => e.domain)).toEqual(["host"]);
  });
});

describe("negotiateTransfer", () => {
  test("prefers a common js pair over a common native pair", () => {
    const result = negotiateTransfer(
      "file",
      [fileNative, fileJs],
      "file",
      [fileNative, fileJs],
      [],
    );
    expect(result.source.kind).toBe(PayloadKind.Js);
    expect(result.converter).toBeUndefined();
    expect(result.path).toBe("file/js -> file/js");
  });

  test("breaks domain ties by the source factory's domain order", () => {
    const destBoth = makeFakeFactory({
      protocol: "dev",
      kind: PayloadKind.Native,
      domains: ["gpu", "host"],
    });
    const result = negotiateTransfer("file", [fileNative], "dev", [destBoth], []);
    expect(result.source.domain).toBe("host");
    expect(result.path).toBe("file/native[host] -> dev/native[host]");
  });

  test("picks the cheapest single converter when no common pair exists", () => {
    const copying = makeConverter({ kind: PayloadKind.Native }, { kind: PayloadKind.Js }, 1);
    const zeroCopy = makeConverter(
      { kind: PayloadKind.Native, domain: "gpu" },
      { kind: PayloadKind.Js },
      0,
    );
    const result = negotiateTransfer("dev", [gpuOnly], "s3", [s3Js], [copying, zeroCopy]);
    expect(result.converter).toBe(zeroCopy);
    expect(result.path).toBe("dev/native[gpu] -> s3/js via native[gpu]->js (zero-copy)");
  });

  test("does not chain converters", () => {
    const toHost = makeConverter(
      { kind: PayloadKind.Native, domain: "gpu" },
      { kind: PayloadKind.Native, domain: "host" },
      1,
    );
    const toJs = makeConverter(
      { kind: PayloadKind.Native, domain: "host" },
      { kind: PayloadKind.Js },
      0,
    );
    expect(() => negotiateTransfer("dev", [gpuOnly], "s3", [s3Js], [toHost, toJs])).toThrow(
      'No common payload kind/domain and no registered converter between "dev" supports [native[gpu]] and "s3" supports [js]',
    );
  });

  test("rejects a payload type mismatch", () => {
    const packets = makeFakeFactory({ protocol: "srt", readPayloadTypes: ["urn:a"] });
    expect(() => negotiateTransfer("srt", [packets], "s3", [s3Js], [])).toThrow(
      'No common payload type: "srt" reads [urn:a], "s3" writes [bytes]',
    );
  });

  test("an explicit kind restricts both sides and only kind-preserving converters", () => {
    const toJs = makeConverter({ kind: PayloadKind.Native }, { kind: PayloadKind.Js }, 0);
    const result = negotiateTransfer(
      "file",
      [fileJs, fileNative],
      "file",
      [fileJs, fileNative],
      [],
      PayloadKind.Native,
    );
    expect(result.source.kind).toBe(PayloadKind.Native);
    expect(() =>
      negotiateTransfer("dev", [gpuOnly], "s3", [s3Js], [toJs], PayloadKind.Native),
    ).toThrow("No common payload kind/domain");
    const gpuToHost = makeConverter(
      { kind: PayloadKind.Native, domain: "gpu" },
      { kind: PayloadKind.Native, domain: "host" },
      1,
    );
    const hostOnly = makeFakeFactory({
      protocol: "h",
      kind: PayloadKind.Native,
      domains: ["host"],
    });
    const converted = negotiateTransfer(
      "dev",
      [gpuOnly],
      "h",
      [hostOnly],
      [gpuToHost],
      PayloadKind.Native,
    );
    expect(converted.converter).toBe(gpuToHost);
  });
});
