import { describe, expect, test } from "bun:test";
import {
  ChunkKind,
  TransientIOError,
  type ItemProperties,
  type IOProvider,
  type JsChunk,
} from "@flowscripter/pluggable-io-framework-api";
import { ConcurrencyLimiter } from "../src/ConcurrencyLimiter.ts";
import { copy, move } from "../src/copyMove.ts";

interface Store {
  files: Map<string, Uint8Array>;
  folders: Set<string>;
}

function makeStore(): Store {
  return { files: new Map(), folders: new Set() };
}

function normalize(path: string): string {
  return path.replace(/^\/+|\/+$/g, "");
}

function isUnderOrEqual(path: string, ancestor: string): boolean {
  if (ancestor === "") return true;
  return path === ancestor || path.startsWith(`${ancestor}/`);
}

function existsAsFolder(store: Store, path: string): boolean {
  const p = normalize(path);
  if (store.folders.has(p)) return true;
  for (const key of [...store.files.keys(), ...store.folders]) {
    if (key !== p && isUnderOrEqual(key, p)) return true;
  }
  return false;
}

interface ProviderOptions {
  readonly id: string;
  readonly supportsRecursiveDirectTransfer?: boolean;
  readonly failFirstNTransfers?: number;
}

function makeFolderProvider(store: Store, options: ProviderOptions): IOProvider {
  let transferAttempts = 0;

  const provider: IOProvider & { id: string } = {
    id: options.id,
    kind: ChunkKind.Js,
    async [Symbol.asyncDispose]() {},

    async *list(path: string, listOptions?: { recursive?: boolean }) {
      const base = normalize(path);
      const seen = new Set<string>();
      const keys = [...store.files.keys(), ...store.folders];
      for (const key of keys) {
        if (!isUnderOrEqual(key, base) || key === base) continue;
        const relative = base === "" ? key : key.slice(base.length + 1);
        const topSegment = relative.split("/")[0] as string;
        if (!listOptions?.recursive && relative !== topSegment) continue;
        const entryPath = listOptions?.recursive ? relative : topSegment;
        if (seen.has(entryPath)) continue;
        seen.add(entryPath);
        const fullPath = base === "" ? entryPath : `${base}/${entryPath}`;
        yield { path: entryPath, properties: await provider.getProperties(fullPath) };
      }
    },

    async getProperties(path: string): Promise<ItemProperties> {
      const p = normalize(path);
      const data = store.files.get(p);
      if (data !== undefined) {
        return { size: data.byteLength, lastModified: undefined, isFolder: false, properties: {} };
      }
      if (existsAsFolder(store, p)) {
        return { size: undefined, lastModified: undefined, isFolder: true, properties: {} };
      }
      throw new Error(`not found: ${path}`);
    },

    async setProperties() {},

    async delete(path: string) {
      const p = normalize(path);
      store.files.delete(p);
      store.folders.delete(p);
      for (const key of store.files.keys()) {
        if (isUnderOrEqual(key, p) && key !== p) store.files.delete(key);
      }
      for (const key of store.folders) {
        if (isUnderOrEqual(key, p) && key !== p) store.folders.delete(key);
      }
    },

    async createFolder(path: string) {
      store.folders.add(normalize(path));
    },

    async getReadableStream(path: string) {
      const data = store.files.get(normalize(path)) ?? new Uint8Array();
      return {
        kind: ChunkKind.Js,
        stream: new ReadableStream<JsChunk>({
          start: (c) => (c.enqueue({ kind: ChunkKind.Js, data }), c.close()),
        }),
      };
    },

    async getWritableStream(path: string) {
      transferAttempts += 1;
      if (options.failFirstNTransfers && transferAttempts <= options.failFirstNTransfers) {
        throw new TransientIOError(`flaky attempt ${transferAttempts}`);
      }
      const p = normalize(path);
      const chunks: Uint8Array[] = [];
      return {
        kind: ChunkKind.Js,
        stream: new WritableStream<JsChunk>({
          write: (chunk) => {
            chunks.push(chunk.data);
          },
          close: () => {
            store.files.set(p, Buffer.concat(chunks));
          },
        }),
      };
    },

    getMultipartReader: async function* () {},
    getMultipartWriter() {
      return { write: async () => {} };
    },

    canDirectTransfer(other: IOProvider) {
      return (other as unknown as { id?: string }).id === options.id;
    },
    supportsRecursiveDirectTransfer: options.supportsRecursiveDirectTransfer,

    async directCopy(sourcePath: string, destPath: string) {
      const sp = normalize(sourcePath);
      const dp = normalize(destPath);
      if (store.files.has(sp)) {
        store.files.set(dp, store.files.get(sp) as Uint8Array);
        return;
      }
      // Folder: recursively copy every descendant.
      store.folders.add(dp);
      for (const [key, data] of [...store.files.entries()]) {
        if (isUnderOrEqual(key, sp) && key !== sp) {
          store.files.set(`${dp}${key.slice(sp.length)}`, data);
        }
      }
      for (const key of [...store.folders]) {
        if (isUnderOrEqual(key, sp) && key !== sp) {
          store.folders.add(`${dp}${key.slice(sp.length)}`);
        }
      }
    },

    async directMove(sourcePath: string, destPath: string) {
      await provider.directCopy?.(sourcePath, destPath);
      await provider.delete(sourcePath);
    },
  };

  return provider;
}

function readFile(store: Store, path: string): string {
  return new TextDecoder().decode(store.files.get(normalize(path)) ?? new Uint8Array());
}

describe("recursive copy - target semantics", () => {
  test("dest that doesn't exist becomes the copy itself", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/nested/b.txt", new TextEncoder().encode("B"));
    const provider = makeFolderProvider(store, { id: "p" });

    await copy(provider, "src", provider, "dest");

    expect(readFile(store, "dest/a.txt")).toBe("A");
    expect(readFile(store, "dest/nested/b.txt")).toBe("B");
    expect(store.files.has("dest/src/a.txt")).toBe(false);
  });

  test("dest that already exists as a folder nests source inside it as dest/<basename>", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.folders.add("dest");
    const provider = makeFolderProvider(store, { id: "p" });

    await copy(provider, "src", provider, "dest");

    expect(readFile(store, "dest/src/a.txt")).toBe("A");
  });

  test("dest that already exists as a file is rejected", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("dest", new TextEncoder().encode("existing file"));
    const provider = makeFolderProvider(store, { id: "p" });

    let thrown: unknown;
    try {
      await copy(provider, "src", provider, "dest");
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(Error);
  });

  test("empty source folders are recreated at the destination via createFolder", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.folders.add("src/emptyDir");
    const provider = makeFolderProvider(store, { id: "p" });

    await copy(provider, "src", provider, "dest");

    expect(store.folders.has("dest/emptyDir")).toBe(true);
  });
});

describe("recursive copy - supportsRecursiveDirectTransfer", () => {
  test("calls directCopy once with the folder path when the provider supports it", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/nested/b.txt", new TextEncoder().encode("B"));
    let directCopyCalls = 0;
    const base = makeFolderProvider(store, { id: "p", supportsRecursiveDirectTransfer: true });
    const provider: IOProvider = {
      ...base,
      async directCopy(sourcePath: string, destPath: string) {
        directCopyCalls += 1;
        await base.directCopy?.(sourcePath, destPath);
      },
    };

    await copy(provider, "src", provider, "dest");

    expect(directCopyCalls).toBe(1);
    expect(readFile(store, "dest/a.txt")).toBe("A");
    expect(readFile(store, "dest/nested/b.txt")).toBe("B");
  });

  test("falls back to listing when supportsRecursiveDirectTransfer is not set, but per-file directCopy is still used", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/b.txt", new TextEncoder().encode("B"));
    let directCopyCalls = 0;
    const base = makeFolderProvider(store, { id: "p" }); // supportsRecursiveDirectTransfer omitted
    const provider: IOProvider = {
      ...base,
      async directCopy(sourcePath: string, destPath: string) {
        directCopyCalls += 1;
        await base.directCopy?.(sourcePath, destPath);
      },
    };

    await copy(provider, "src", provider, "dest");

    // One directCopy call per file entry, not one for the whole folder.
    expect(directCopyCalls).toBe(2);
    expect(readFile(store, "dest/a.txt")).toBe("A");
    expect(readFile(store, "dest/b.txt")).toBe("B");
  });
});

describe("recursive copy - concurrency", () => {
  test("a shared limiter bounds concurrent entry transfers across the whole recursive copy", async () => {
    const store = makeStore();
    for (let i = 0; i < 6; i += 1) {
      store.files.set(`src/file${i}.txt`, new TextEncoder().encode(`data${i}`));
    }
    const source = makeFolderProvider(store, { id: "source" });
    const sinkStore = makeStore();
    const sink = makeFolderProvider(sinkStore, { id: "sink" });

    let active = 0;
    let maxActive = 0;
    const trackedSink: IOProvider = {
      ...sink,
      async getWritableStream(path: string) {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        const handle = await sink.getWritableStream(path);
        const originalWriter = (handle.stream as WritableStream<JsChunk>).getWriter();
        return {
          kind: handle.kind,
          stream: new WritableStream<JsChunk>({
            write: (chunk) => originalWriter.write(chunk),
            close: async () => {
              await originalWriter.close();
              active -= 1;
            },
          }),
        };
      },
    };

    const limiter = new ConcurrencyLimiter(2);
    await copy(source, "src", trackedSink, "dest", { concurrencyLimiter: limiter });

    expect(maxActive).toBeLessThanOrEqual(2);
    expect(readFile(sinkStore, "dest/file0.txt")).toBe("data0");
  });
});

describe("recursive copy - progress", () => {
  test("reports a growing itemsProcessed/totalItems for the overall folder operation", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/b.txt", new TextEncoder().encode("B"));
    store.files.set("src/c.txt", new TextEncoder().encode("C"));
    const provider = makeFolderProvider(store, { id: "p" });

    const events: { operationId: string; itemsProcessed?: number; totalItems?: number }[] = [];
    await copy(provider, "src", provider, "dest", {
      telemetry: { onProgress: (event) => events.push(event) },
    });

    const folderEvents = events.filter((e) => e.totalItems !== undefined);
    expect(folderEvents.length).toBeGreaterThan(0);
    expect(folderEvents.at(-1)?.itemsProcessed).toBe(3);
    expect(folderEvents.at(-1)?.totalItems).toBe(3);
  });
});

describe("recursive move", () => {
  test("deletes the source root only once, after every entry has succeeded", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/b.txt", new TextEncoder().encode("B"));
    const source = makeFolderProvider(store, { id: "source" });
    const sinkStore = makeStore();
    const sink = makeFolderProvider(sinkStore, { id: "sink" });

    await move(source, "src", sink, "dest");

    expect(readFile(sinkStore, "dest/a.txt")).toBe("A");
    expect(readFile(sinkStore, "dest/b.txt")).toBe("B");
    expect(store.files.has("src/a.txt")).toBe(false);
    expect(store.files.has("src/b.txt")).toBe(false);
    expect(existsAsFolder(store, "src")).toBe(false);
  });

  test("leaves the source tree intact if any entry fails", async () => {
    const store = makeStore();
    store.files.set("src/a.txt", new TextEncoder().encode("A"));
    store.files.set("src/b.txt", new TextEncoder().encode("B"));
    const source = makeFolderProvider(store, { id: "source" });
    const sinkStore = makeStore();
    const sink = makeFolderProvider(sinkStore, { id: "sink" });
    const failingSink: IOProvider = {
      ...sink,
      async getWritableStream(path: string): Promise<never> {
        if (path.includes("b.txt")) {
          throw new Error("permanent failure writing b.txt");
        }
        return sink.getWritableStream(path) as never;
      },
    };

    let thrown: unknown;
    try {
      await move(source, "src", failingSink, "dest", { retry: { maxRetries: 0 } });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(Error);
    expect(store.files.has("src/a.txt")).toBe(true);
    expect(store.files.has("src/b.txt")).toBe(true);
  });
});
