import { describe, expect, test } from "bun:test";
import { PermanentIOError } from "@flowscripter/pluggable-io-framework-api";
import { copy, move } from "../../src/transfer/copyMove.ts";
import { makeMemoryProvider, makeStore, readText } from "../fixtures/memoryProvider.ts";
import { container, entry, pattern } from "../fixtures/targets.ts";

function setup() {
  const sourceStore = makeStore({ "dir/a.txt": "A", "dir/b.md": "B", "dir/sub/c.txt": "C" });
  const sinkStore = makeStore();
  sinkStore.folders.add("out");
  return {
    sourceStore,
    sinkStore,
    source: makeMemoryProvider(sourceStore, { id: "source" }),
    sink: makeMemoryProvider(sinkStore, { id: "sink" }),
  };
}

async function rejection(promise: Promise<unknown>): Promise<Error> {
  return (await promise.catch((error: unknown) => error)) as Error;
}

describe("copy targets", () => {
  test("entry -> entry writes exactly the destination key", async () => {
    const { source, sink, sinkStore } = setup();
    const result = await copy(source, entry("dir/a.txt"), sink, entry("renamed.txt"));
    expect(readText(sinkStore, "renamed.txt")).toBe("A");
    expect(result).toEqual({ stopped: false, bytes: 1, items: 1, path: "js -> js, stream" });
  });

  test("entry -> container writes the source basename inside the container", async () => {
    const { source, sink, sinkStore } = setup();
    await copy(source, entry("dir/a.txt"), sink, container("out"));
    expect(readText(sinkStore, "out/a.txt")).toBe("A");
  });

  test("container -> container copies recursively", async () => {
    const { source, sink, sinkStore } = setup();
    const result = await copy(source, container("dir"), sink, container("copy"));
    expect(readText(sinkStore, "copy/sub/c.txt")).toBe("C");
    expect(result.bytes).toBe(3);
  });

  test("pattern -> container copies the matching entries", async () => {
    const { source, sink, sinkStore } = setup();
    await copy(source, pattern("dir", "*.txt"), sink, container("out"));
    expect(readText(sinkStore, "out/a.txt")).toBe("A");
    expect(sinkStore.files.has("out/b.md")).toBe(false);
  });

  test("container -> entry, pattern -> entry and pattern destinations are rejected", async () => {
    const { source, sink } = setup();
    expect(await rejection(copy(source, container("dir"), sink, entry("x")))).toBeInstanceOf(
      PermanentIOError,
    );
    expect((await rejection(copy(source, container("dir"), sink, entry("x")))).message).toBe(
      "Cannot transfer a container onto an entry",
    );
    expect((await rejection(copy(source, pattern("dir", "*"), sink, entry("x")))).message).toBe(
      "Cannot transfer a pattern of entries onto a single entry",
    );
    expect(
      (await rejection(copy(source, entry("dir/a.txt"), sink, pattern("out", "*")))).message,
    ).toBe("A destination cannot be a pattern");
  });

  test("the declared source variant must match the source", async () => {
    const { source, sink } = setup();
    expect((await rejection(copy(source, entry("dir"), sink, entry("x")))).message).toBe(
      'Source "dir" is declared as an entry but is a container',
    );
    expect(
      (await rejection(copy(source, container("dir/a.txt"), sink, container("x")))).message,
    ).toBe('Source "dir/a.txt" is declared as a container but is an entry');
    expect(
      (await rejection(copy(source, pattern("dir/a.txt", "*"), sink, container("x")))).message,
    ).toBe('Source "dir/a.txt" is declared as a pattern container but is an entry');
  });
});

describe("move targets", () => {
  test("entry -> entry copies then deletes the source", async () => {
    const { source, sink, sourceStore, sinkStore } = setup();
    await move(source, entry("dir/a.txt"), sink, entry("moved.txt"));
    expect(readText(sinkStore, "moved.txt")).toBe("A");
    expect(sourceStore.files.has("dir/a.txt")).toBe(false);
  });

  test("container -> container moves recursively and removes the source container", async () => {
    const { source, sink, sourceStore, sinkStore } = setup();
    await move(source, container("dir"), sink, container("moved"));
    expect(readText(sinkStore, "moved/a.txt")).toBe("A");
    expect(sourceStore.files.size).toBe(0);
  });

  test("pattern -> container moves only the matching entries", async () => {
    const { source, sink, sourceStore, sinkStore } = setup();
    await move(source, pattern("dir", "*.md"), sink, container("out"));
    expect(readText(sinkStore, "out/b.md")).toBe("B");
    expect(sourceStore.files.has("dir/b.md")).toBe(false);
    expect(sourceStore.files.has("dir/a.txt")).toBe(true);
  });
});
