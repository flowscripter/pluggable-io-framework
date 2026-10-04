import { describe, expect, test } from "bun:test";
import { detectProtocol } from "../../src/registry/detectProtocol.ts";

describe("detectProtocol", () => {
  test("defaults to file for bare paths", () => {
    expect(detectProtocol("/tmp/a.txt")).toBe("file");
    expect(detectProtocol("relative/a.txt")).toBe("file");
  });

  test("returns the lower-cased scheme", () => {
    expect(detectProtocol("file:///tmp")).toBe("file");
    expect(detectProtocol("S3://bucket/key")).toBe("s3");
    expect(detectProtocol("https://host/a")).toBe("https");
  });

  test("does not read a Windows drive letter as a scheme", () => {
    expect(detectProtocol("C:\\foo")).toBe("file");
    expect(detectProtocol("c:/foo")).toBe("file");
  });

  test("uses the token before the first + for composite schemes", () => {
    expect(detectProtocol("tams+https://host/flow")).toBe("tams");
    expect(detectProtocol("wrap+file:///tmp")).toBe("wrap");
  });
});
