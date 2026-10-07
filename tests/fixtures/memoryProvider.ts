import {
  type EntryProperties,
  type IOProvider,
  type Item,
  type MultipartWriter,
  type Part,
  type PartSizeConstraints,
  PayloadKind,
  type ResumableWritable,
  type ResumeToken,
  type StreamHandle,
} from "@flowscripter/pluggable-io-framework-api";

export interface MemoryStore {
  readonly files: Map<string, Uint8Array>;
  readonly folders: Set<string>;
  readonly events: string[];
}

export function makeStore(files: Record<string, string> = {}): MemoryStore {
  const store: MemoryStore = { files: new Map(), folders: new Set(), events: [] };
  for (const [path, text] of Object.entries(files)) {
    store.files.set(path, new TextEncoder().encode(text));
  }
  return store;
}

export function normalize(path: string): string {
  return path.replace(/^\/+|\/+$/g, "");
}

function isUnderOrEqual(path: string, ancestor: string): boolean {
  if (ancestor === "") return true;
  return path === ancestor || path.startsWith(`${ancestor}/`);
}

export function existsAsFolder(store: MemoryStore, path: string): boolean {
  const p = normalize(path);
  if (store.folders.has(p)) return true;
  for (const key of [...store.files.keys(), ...store.folders]) {
    if (key !== p && isUnderOrEqual(key, p)) return true;
  }
  return false;
}

export function readText(store: MemoryStore, path: string): string {
  return new TextDecoder().decode(store.files.get(normalize(path)) ?? new Uint8Array());
}

export function jsItem(text: string): Item<PayloadKind.Js> {
  return { payload: { kind: PayloadKind.Js, data: new TextEncoder().encode(text) } };
}

export interface MemoryProviderOptions {
  readonly id?: string;
  /** Bytes per item on reads. Defaults to the whole entry in one item. */
  readonly itemSize?: number;
  /** Readable handles implement `RangeReadable`. Defaults to `true`. */
  readonly rangeReadable?: boolean;
  /** Writes land immediately and writable handles produce resume tokens. */
  readonly resumable?: boolean;
  readonly multipart?: boolean;
  readonly partSizeConstraints?: (totalSize: number) => PartSizeConstraints;
  readonly recordedPartSizes?: number[];
  /** `canDirectTransfer` is true for another provider with the same id. */
  readonly direct?: boolean;
  readonly supportsRecursiveDirectTransfer?: boolean;
  readonly bounded?: boolean;
  readonly payloadType?: string;
  /** Called before each item is read; may throw. */
  readonly onRead?: (path: string, offset: number) => void;
  /** Called before each item is written; may throw. */
  readonly onWrite?: (path: string, committed: number) => void;
  /** Called when a readable is opened; may throw. */
  readonly onOpenRead?: (path: string) => void;
  /** Called when a writable is opened; may throw. */
  readonly onOpenWrite?: (path: string, resume?: ResumeToken) => void;
}

export type MemoryProvider = IOProvider<PayloadKind.Js> & { readonly id: string };

/** An in-memory provider over `/`-separated keys, with configurable capabilities. */
export function makeMemoryProvider(
  store: MemoryStore,
  options: MemoryProviderOptions = {},
): MemoryProvider {
  const id = options.id ?? "memory";

  function readStream(
    path: string,
    start: number,
    end: number,
  ): ReadableStream<Item<PayloadKind.Js>> {
    let offset = start;
    return new ReadableStream<Item<PayloadKind.Js>>({
      pull(controller) {
        const data = store.files.get(path) ?? new Uint8Array();
        const stop = Math.min(end, data.byteLength);
        if (offset >= stop) {
          controller.close();
          return;
        }
        options.onRead?.(path, offset);
        const next = Math.min(offset + (options.itemSize ?? Infinity), stop);
        controller.enqueue({
          payload: { kind: PayloadKind.Js, data: data.subarray(offset, next) },
        });
        offset = next;
      },
    });
  }

  const provider: MemoryProvider = {
    id,
    kind: PayloadKind.Js,
    async [Symbol.asyncDispose]() {
      store.events.push(`dispose:${id}`);
    },

    async *list(path: string, listOptions?: { recursive?: boolean; regex?: RegExp }) {
      const base = normalize(path);
      const seen = new Set<string>();
      for (const key of [...store.files.keys(), ...store.folders]) {
        if (!isUnderOrEqual(key, base) || key === base) continue;
        const relative = base === "" ? key : key.slice(base.length + 1);
        const topSegment = relative.split("/")[0] as string;
        const entryPath = listOptions?.recursive ? relative : topSegment;
        if (seen.has(entryPath)) continue;
        if (listOptions?.regex && !listOptions.regex.test(entryPath)) continue;
        seen.add(entryPath);
        const fullPath = base === "" ? entryPath : `${base}/${entryPath}`;
        yield { path: entryPath, properties: await provider.getProperties(fullPath) };
      }
    },

    async getProperties(path: string): Promise<EntryProperties> {
      const p = normalize(path);
      const data = store.files.get(p);
      if (data !== undefined) {
        return {
          size: data.byteLength,
          lastModified: undefined,
          isContainer: false,
          properties: {},
        };
      }
      if (existsAsFolder(store, p)) {
        return { size: undefined, lastModified: undefined, isContainer: true, properties: {} };
      }
      throw new Error(`not found: ${path}`);
    },

    async delete(path: string) {
      const p = normalize(path);
      store.events.push(`delete:${p}`);
      for (const key of Array.from(store.files.keys())) {
        if (isUnderOrEqual(key, p)) store.files.delete(key);
      }
      for (const key of Array.from(store.folders)) {
        if (isUnderOrEqual(key, p)) store.folders.delete(key);
      }
    },

    async createContainer(path: string) {
      store.folders.add(normalize(path));
    },

    async getReadableStream(path: string) {
      const p = normalize(path);
      options.onOpenRead?.(p);
      const handle: StreamHandle<PayloadKind.Js> & {
        readRange?: (start: number, end: number) => Promise<ReadableStream<Item<PayloadKind.Js>>>;
      } = {
        kind: PayloadKind.Js,
        stream: readStream(p, 0, Number.MAX_SAFE_INTEGER),
        bounded: options.bounded,
        payloadType: options.payloadType,
      };
      if (options.rangeReadable !== false) {
        handle.readRange = async (start, end) => readStream(p, start, end);
      }
      return handle;
    },

    async getWritableStream(path: string, opts?: { resume?: ResumeToken }) {
      const p = normalize(path);
      options.onOpenWrite?.(p, opts?.resume);
      if (options.resumable) {
        if (!opts?.resume) store.files.set(p, new Uint8Array());
        let committed = store.files.get(p)?.byteLength ?? 0;
        const startOffset = committed;
        return {
          kind: PayloadKind.Js,
          startOffset,
          resumeToken: () => ({ offset: committed }),
          stream: new WritableStream<Item<PayloadKind.Js>>({
            write: (item) => {
              options.onWrite?.(p, committed);
              const existing = store.files.get(p) ?? new Uint8Array();
              store.files.set(p, Buffer.concat([existing, item.payload.data]));
              committed += item.payload.data.byteLength;
            },
            close: () => {
              store.events.push(`close:${p}`);
            },
            abort: () => {
              store.events.push(`abort:${p}`);
            },
          }),
        };
      }
      const chunks: Uint8Array[] = [];
      let committed = 0;
      return {
        kind: PayloadKind.Js,
        stream: new WritableStream<Item<PayloadKind.Js>>({
          write: (item) => {
            options.onWrite?.(p, committed);
            chunks.push(item.payload.data);
            committed += item.payload.data.byteLength;
          },
          close: () => {
            store.files.set(p, Buffer.concat(chunks));
            store.events.push(`close:${p}`);
          },
          abort: () => {
            store.events.push(`abort:${p}`);
          },
        }),
      };
    },

    getPartSizeConstraints: options.partSizeConstraints,

    canDirectTransfer: options.direct
      ? (other: IOProvider) => (other as Partial<MemoryProvider>).id === id
      : undefined,
    supportsRecursiveDirectTransfer: options.supportsRecursiveDirectTransfer,
    directCopy: options.direct
      ? async (sourcePath: string, destPath: string) => {
          const sp = normalize(sourcePath);
          const dp = normalize(destPath);
          store.events.push(`directCopy:${sp}->${dp}`);
          for (const [key, data] of Array.from(store.files.entries())) {
            if (isUnderOrEqual(key, sp)) store.files.set(`${dp}${key.slice(sp.length)}`, data);
          }
          for (const key of Array.from(store.folders)) {
            if (isUnderOrEqual(key, sp)) store.folders.add(`${dp}${key.slice(sp.length)}`);
          }
          if (!store.files.has(sp)) store.folders.add(dp);
        }
      : undefined,
    directMove: options.direct
      ? async (sourcePath: string, destPath: string) => {
          store.events.push(`directMove:${normalize(sourcePath)}->${normalize(destPath)}`);
          await provider.directCopy?.(sourcePath, destPath);
          await provider.delete?.(sourcePath);
        }
      : undefined,
  };

  if (options.multipart) {
    // Committed parts per upload id, kept across writers so a resumed writer sees them.
    const uploads = new Map<string, Map<number, Uint8Array>>();
    provider.getMultipartWriter = (
      path: string,
      partSize: number,
      opts?: { resume?: ResumeToken },
    ) => {
      options.recordedPartSizes?.push(partSize);
      const p = normalize(path);
      const uploadId = (opts?.resume?.state as string | undefined) ?? crypto.randomUUID();
      const committed = uploads.get(uploadId) ?? new Map<number, Uint8Array>();
      uploads.set(uploadId, committed);
      store.events.push(`multipart:${p}:${opts?.resume ? "resume" : "start"}`);
      const writer: MultipartWriter<PayloadKind.Js> & Partial<ResumableWritable> = {
        async write(parts: AsyncIterable<Part<PayloadKind.Js>>) {
          for await (const part of parts) {
            const reader = (part.stream as ReadableStream<Item<PayloadKind.Js>>).getReader();
            const chunks: Uint8Array[] = [];
            for (;;) {
              const { done, value } = await reader.read();
              if (done) break;
              chunks.push(value.payload.data);
            }
            store.events.push(`part:${p}:${part.index}`);
            options.onWrite?.(p, part.offset);
            committed.set(part.index, Buffer.concat(chunks));
            await part.complete();
          }
          const indexes = [...committed.keys()].sort((a, b) => a - b);
          store.files.set(p, Buffer.concat(indexes.map((index) => committed.get(index)!)));
        },
      };
      if (options.resumable) {
        writer.resumeToken = () => {
          let offset = 0;
          for (let index = 0; committed.has(index); index += 1) {
            offset += committed.get(index)!.byteLength;
          }
          return { offset, state: uploadId };
        };
      }
      return writer;
    };
  }

  return provider;
}

/** A sink that only records the items written to it, for kind/converter tests. */
export function makeRecordingSink(kind: PayloadKind): IOProvider & { readonly written: Item[] } {
  const written: Item[] = [];
  return {
    written,
    kind,
    async [Symbol.asyncDispose]() {},
    async getProperties() {
      return { size: undefined, lastModified: undefined, isContainer: false, properties: {} };
    },
    async getReadableStream() {
      return { kind, stream: new ReadableStream<Item>() };
    },
    async getWritableStream() {
      return {
        kind,
        stream: new WritableStream<Item>({
          write: (item) => {
            written.push(item);
          },
        }),
      };
    },
  };
}
