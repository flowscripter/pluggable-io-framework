import { describe, expect, test } from "bun:test";
import {
  type IOProvider,
  type Item,
  type PayloadKind,
  PermanentIOError,
} from "@flowscripter/pluggable-io-framework-api";
import { ConcurrencyLimiter } from "../../src/concurrency/ConcurrencyLimiter.ts";
import { copy, move } from "../../src/transfer/copyMove.ts";
import {
  existsAsFolder,
  makeMemoryProvider,
  makeStore,
  type MemoryStore,
  readText,
} from "../fixtures/memoryProvider.ts";
import { container } from "../fixtures/targets.ts";

async function rejection(promise: Promise<unknown>): Promise<Error> {
  return (await promise.catch((error: unknown) => error)) as Error;
}

describe("recursive copy - target semantics", () => {
  test("dest that doesn't exist becomes the copy itself", async () => {
    const store = makeStore({ "src/a.txt": "A", "src/nested/b.txt": "B" });
    const provider = makeMemoryProvider(store);

    await copy(provider, container("src"), provider, container("dest"));

    expect(readText(store, "dest/a.txt")).toBe("A");
    expect(readText(store, "dest/nested/b.txt")).toBe("B");
    expect(store.files.has("dest/src/a.txt")).toBe(false);
  });

  test("dest that already exists as a container nests source inside it as dest/<basename>", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    store.folders.add("dest");
    const provider = makeMemoryProvider(store);

    await copy(provider, container("src"), provider, container("dest"));

    expect(readText(store, "dest/src/a.txt")).toBe("A");
  });

  test("dest that already exists as an entry is rejected", async () => {
    const store = makeStore({ "src/a.txt": "A", dest: "existing file" });
    const provider = makeMemoryProvider(store);

    const error = await rejection(copy(provider, container("src"), provider, container("dest")));

    expect(error).toBeInstanceOf(PermanentIOError);
    expect(readText(store, "dest")).toBe("existing file");
  });

  test("empty source containers are recreated at the destination via createContainer", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    store.folders.add("src/emptyDir");
    const provider = makeMemoryProvider(store);

    await copy(provider, container("src"), provider, container("dest"));

    expect(store.folders.has("dest/emptyDir")).toBe(true);
  });
});

describe("recursive copy - supportsRecursiveDirectTransfer", () => {
  test("calls directCopy once with the container key when the provider supports it", async () => {
    const store = makeStore({ "src/a.txt": "A", "src/nested/b.txt": "B" });
    const provider = makeMemoryProvider(store, {
      direct: true,
      supportsRecursiveDirectTransfer: true,
    });

    const result = await copy(provider, container("src"), provider, container("dest"));

    expect(store.events.filter((e) => e.startsWith("directCopy"))).toEqual([
      "directCopy:src->dest",
    ]);
    expect(readText(store, "dest/nested/b.txt")).toBe("B");
    expect(result.path).toBe("js -> js, direct");
  });

  test("falls back to listing when supportsRecursiveDirectTransfer is not set, but per-entry directCopy is still used", async () => {
    const store = makeStore({ "src/a.txt": "A", "src/b.txt": "B" });
    const provider = makeMemoryProvider(store, { direct: true });

    await copy(provider, container("src"), provider, container("dest"));

    // One directCopy call per entry, not one for the whole container.
    expect(store.events.filter((e) => e.startsWith("directCopy")).length).toBe(2);
    expect(readText(store, "dest/a.txt")).toBe("A");
    expect(readText(store, "dest/b.txt")).toBe("B");
  });

  test("directTransfer false streams every entry", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const provider = makeMemoryProvider(store, {
      direct: true,
      supportsRecursiveDirectTransfer: true,
    });

    await copy(provider, container("src"), provider, container("dest"), { directTransfer: false });

    expect(store.events.some((e) => e.startsWith("directCopy"))).toBe(false);
    expect(readText(store, "dest/a.txt")).toBe("A");
  });
});

describe("recursive copy - concurrency", () => {
  test("a shared limiter bounds concurrent entry transfers across the whole recursive copy", async () => {
    const store = makeStore();
    for (let i = 0; i < 6; i += 1) {
      store.files.set(`src/file${i}.txt`, new TextEncoder().encode(`data${i}`));
    }
    const source = makeMemoryProvider(store, { id: "source" });
    const sinkStore = makeStore();
    const sink = makeMemoryProvider(sinkStore, { id: "sink" });

    let active = 0;
    let maxActive = 0;
    const trackedSink: IOProvider = {
      ...sink,
      async getWritableStream(path: string) {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        const handle = await sink.getWritableStream(path);
        const originalWriter = (handle.stream as WritableStream<Item<PayloadKind.Js>>).getWriter();
        return {
          kind: handle.kind,
          stream: new WritableStream<Item<PayloadKind.Js>>({
            write: (item) => originalWriter.write(item),
            close: async () => {
              await originalWriter.close();
              active -= 1;
            },
          }),
        };
      },
    };

    await copy(source, container("src"), trackedSink, container("dest"), {
      concurrencyLimiter: new ConcurrencyLimiter(2),
    });

    expect(maxActive).toBeLessThanOrEqual(2);
    expect(readText(sinkStore, "dest/file0.txt")).toBe("data0");
  });
});

describe("recursive copy - progress", () => {
  test("reports a growing entriesProcessed/totalEntries for the overall container operation", async () => {
    const store = makeStore({ "src/a.txt": "A", "src/b.txt": "B", "src/c.txt": "C" });
    const provider = makeMemoryProvider(store);

    const events: { entriesProcessed?: number; totalEntries?: number }[] = [];
    const result = await copy(provider, container("src"), provider, container("dest"), {
      telemetry: { onProgress: (event) => events.push(event) },
    });

    const containerEvents = events.filter((e) => e.totalEntries !== undefined);
    expect(containerEvents.at(-1)?.entriesProcessed).toBe(3);
    expect(containerEvents.at(-1)?.totalEntries).toBe(3);
    expect(result).toEqual({ stopped: false, bytes: 3, items: 3, path: "js -> js" });
  });
});

describe("recursive copy - lifecycle", () => {
  test("stop starts no new entries and reports stopped", async () => {
    const store = makeStore({ "src/a.txt": "A", "src/b.txt": "B" });
    const provider = makeMemoryProvider(store);
    const stop = new AbortController();
    stop.abort();
    const result = await copy(provider, container("src"), provider, container("dest"), {
      stop: stop.signal,
    });
    expect(result.stopped).toBe(true);
    expect(store.files.has("dest/a.txt")).toBe(false);
  });

  test("signal aborts the transfer", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const provider = makeMemoryProvider(store);
    const signal = new AbortController();
    signal.abort();
    const error = await rejection(
      copy(provider, container("src"), provider, container("dest"), { signal: signal.signal }),
    );
    expect(error.name).toBe("AbortError");
  });

  test("signal aborts before a recursive direct transfer", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const provider = makeMemoryProvider(store, {
      direct: true,
      supportsRecursiveDirectTransfer: true,
    });
    const signal = new AbortController();
    signal.abort();
    const error = await rejection(
      copy(provider, container("src"), provider, container("dest"), { signal: signal.signal }),
    );
    expect(error.name).toBe("AbortError");
  });

  test("a source without list cannot be copied recursively", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const { list: _list, ...noList } = makeMemoryProvider(store);
    expect(
      (await rejection(copy(noList, container("src"), noList, container("dest")))).message,
    ).toBe("The source provider does not support listing containers");
  });
});

function setupMove(): {
  store: MemoryStore;
  sinkStore: MemoryStore;
  source: IOProvider;
  sink: IOProvider;
} {
  const store = makeStore({ "src/a.txt": "A", "src/b.txt": "B" });
  const sinkStore = makeStore();
  return {
    store,
    sinkStore,
    source: makeMemoryProvider(store, { id: "source" }),
    sink: makeMemoryProvider(sinkStore, { id: "sink" }),
  };
}

describe("recursive move", () => {
  test("deletes the source root only once, after every entry has succeeded", async () => {
    const { store, sinkStore, source, sink } = setupMove();

    await move(source, container("src"), sink, container("dest"));

    expect(readText(sinkStore, "dest/a.txt")).toBe("A");
    expect(readText(sinkStore, "dest/b.txt")).toBe("B");
    expect(store.events.filter((e) => e.startsWith("delete"))).toEqual(["delete:src"]);
    expect(existsAsFolder(store, "src")).toBe(false);
  });

  test("leaves the source tree intact if any entry fails", async () => {
    const { store, source, sink } = setupMove();
    const failingSink: IOProvider = {
      ...sink,
      async getWritableStream(path: string) {
        if (path.includes("b.txt")) {
          throw new Error("permanent failure writing b.txt");
        }
        return sink.getWritableStream(path);
      },
    };

    const error = await rejection(
      move(source, container("src"), failingSink, container("dest"), { retry: { maxRetries: 0 } }),
    );

    expect(error.message).toBe("permanent failure writing b.txt");
    expect(store.files.has("src/a.txt")).toBe(true);
    expect(store.files.has("src/b.txt")).toBe(true);
  });

  test("a stopped move keeps the source", async () => {
    const { store, source, sink } = setupMove();
    const stop = new AbortController();
    stop.abort();
    await move(source, container("src"), sink, container("dest"), { stop: stop.signal });
    expect(store.files.has("src/a.txt")).toBe(true);
  });

  test("a source without delete cannot be moved", async () => {
    const { source, sink } = setupMove();
    const { delete: _delete, ...noDelete } = source;
    expect(
      (await rejection(move(noDelete, container("src"), sink, container("dest")))).message,
    ).toBe("The source provider does not support delete, so it cannot move");
  });
});

class ClassContainerProvider implements IOProvider<PayloadKind.Js> {
  readonly #delegate: IOProvider<PayloadKind.Js>;
  public readonly kind;
  public readonly supportsRecursiveDirectTransfer = true;

  public constructor(store: MemoryStore) {
    this.#delegate = makeMemoryProvider(store, { id: "class", direct: true });
    this.kind = this.#delegate.kind;
  }

  public async [Symbol.asyncDispose](): Promise<void> {}

  public getProperties(path: string) {
    return this.#delegate.getProperties(path);
  }

  public async delete(path: string) {
    await this.#delegate.delete?.(path);
  }

  public getReadableStream(path: string) {
    return this.#delegate.getReadableStream(path);
  }

  public getWritableStream(path: string) {
    return this.#delegate.getWritableStream(path);
  }

  public canDirectTransfer(other: IOProvider): boolean {
    return (other as unknown) === this;
  }

  public async directCopy(sourcePath: string, destPath: string): Promise<void> {
    await this.#delegate.directCopy?.(sourcePath, destPath);
  }

  public async directMove(sourcePath: string, destPath: string): Promise<void> {
    await this.#delegate.directMove?.(sourcePath, destPath);
  }
}

describe("recursive direct transfer - class-based provider", () => {
  test("copy calls a container-aware directCopy with the provider as `this`", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const provider = new ClassContainerProvider(store);

    await copy(provider, container("src"), provider, container("dest"));

    expect(readText(store, "dest/a.txt")).toBe("A");
  });

  test("move calls a container-aware directMove with the provider as `this`", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    const provider = new ClassContainerProvider(store);

    await move(provider, container("src"), provider, container("dest"));

    expect(readText(store, "dest/a.txt")).toBe("A");
    expect(store.files.has("src/a.txt")).toBe(false);
  });

  test("copy into an existing container nests the source as dest/<basename>", async () => {
    const store = makeStore({ "src/a.txt": "A" });
    store.folders.add("dest");
    const provider = new ClassContainerProvider(store);

    await copy(provider, container("src"), provider, container("dest"));

    expect(readText(store, "dest/src/a.txt")).toBe("A");
  });
});
