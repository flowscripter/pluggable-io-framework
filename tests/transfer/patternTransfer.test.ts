import { describe, expect, test } from "bun:test";
import { PermanentIOError, type IOProvider } from "@flowscripter/pluggable-io-framework-api";
import { copy, move } from "../../src/transfer/copyMove.ts";
import { patternTransfer } from "../../src/transfer/patternTransfer.ts";
import { makeMemoryProvider, makeStore, readText } from "../fixtures/memoryProvider.ts";
import { container, pattern } from "../fixtures/targets.ts";

function setup() {
  const sourceStore = makeStore({
    "dir/a.txt": "A",
    "dir/b.txt": "BB",
    "dir/c.md": "C",
    "dir/sub.txt/inner.txt": "I",
  });
  const sinkStore = makeStore();
  return {
    sourceStore,
    sinkStore,
    source: makeMemoryProvider(sourceStore, { id: "source" }),
    sink: makeMemoryProvider(sinkStore, { id: "sink" }),
  };
}

describe("patternTransfer", () => {
  test("copies matching entries, skipping non-matching entries and containers", async () => {
    const { source, sink, sinkStore } = setup();
    const events: { entriesProcessed?: number; totalEntries?: number }[] = [];
    const result = await copy(source, pattern("dir", "*.txt"), sink, container("out"), {
      telemetry: { onProgress: (event) => events.push(event) },
    });
    expect([...sinkStore.files.keys()].sort()).toEqual(["out/a.txt", "out/b.txt"]);
    expect(readText(sinkStore, "out/b.txt")).toBe("BB");
    expect(result).toEqual({ stopped: false, bytes: 3, items: 2, path: "js -> js" });
    expect(events.filter((e) => e.totalEntries !== undefined).at(-1)).toMatchObject({
      entriesProcessed: 2,
      totalEntries: 2,
    });
  });

  test("move deletes each transferred entry", async () => {
    const { source, sink, sourceStore } = setup();
    await move(source, pattern("dir", "?.txt"), sink, container("out"));
    expect(sourceStore.files.has("dir/a.txt")).toBe(false);
    expect(sourceStore.files.has("dir/c.md")).toBe(true);
  });

  test("stop transfers nothing further and reports stopped", async () => {
    const { source, sink, sinkStore } = setup();
    const stop = new AbortController();
    stop.abort();
    const result = await copy(source, pattern("dir", "*"), sink, container("out"), {
      stop: stop.signal,
    });
    expect(result.stopped).toBe(true);
    expect(sinkStore.files.size).toBe(0);
  });

  test("a source without list is rejected", async () => {
    const { source, sink } = setup();
    const { list: _list, ...noList } = source;
    await expect(patternTransfer(noList, "dir", "*", sink, "out", {}, "copy")).rejects.toThrow(
      "does not support listing",
    );
  });
});

describe("patternTransfer - entry path validation", () => {
  const BAD: [string, string][] = [
    ["empty", ""],
    ["absolute", "/etc/x"],
    ["drive-letter", "C:/x"],
    ["backslash", "a\\b"],
    ["NUL", "a\0b"],
    ["parent segment", "../x"],
    ["nested parent segment", "a/../../x"],
    ["dot segment", "a/./b"],
  ];

  for (const type of ["copy", "move"] as const) {
    const run = type === "copy" ? copy : move;

    test(`${type} accepts normal names`, async () => {
      const { source, sink, sinkStore } = setup();
      await run(source, pattern("dir", "*.md"), sink, container("out"));
      expect([...sinkStore.files.keys()]).toEqual(["out/c.md"]);
    });

    for (const [name, path] of BAD) {
      test(`${type} rejects ${name} entry path and writes nothing`, async () => {
        const { source, sink, sinkStore, sourceStore } = setup();
        const properties = await source.getProperties("dir/a.txt");
        const evil = {
          ...source,
          async *list() {
            yield { path, properties };
          },
        } as IOProvider;
        const error = (await run(evil, pattern("dir", "*"), sink, container("out")).catch(
          (e: unknown) => e,
        )) as Error;
        expect(error).toBeInstanceOf(PermanentIOError);
        expect(error.message).toContain("Unsafe entry path");
        expect(sinkStore.files.size).toBe(0);
        expect(sourceStore.files.has("dir/a.txt")).toBe(true);
      });
    }
  }
});
