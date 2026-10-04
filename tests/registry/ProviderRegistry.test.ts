import { describe, expect, test } from "bun:test";
import {
  type IOProvider,
  type IOProviderFactory,
  PayloadKind,
  PermanentIOError,
  type ProviderContext,
} from "@flowscripter/pluggable-io-framework-api";
import { z } from "zod";
import { MAX_RESOLUTION_DEPTH, ProviderRegistry } from "../../src/registry/ProviderRegistry.ts";
import {
  makeConverter,
  makeFakeFactory,
  makeFakePluginManager,
} from "../fixtures/fakeFactories.ts";

async function registryOf(
  factories: IOProviderFactory[],
  converters = [] as ReturnType<typeof makeConverter>[],
): Promise<ProviderRegistry> {
  const registry = new ProviderRegistry(makeFakePluginManager(factories, converters));
  await registry.discover();
  return registry;
}

/** `wrap+<inner>` resolves `<inner>` through the context resolver and disposes it with itself. */
function makeWrapFactory(events: string[]): IOProviderFactory {
  return {
    protocol: "wrap",
    kind: PayloadKind.Js,
    configSchema: z.object({ inner: z.string() }),
    locationSchema: z.object({ inner: z.string() }),
    propertySchema: z.object({}),
    settablePropertySchema: z.object({}),
    parseLocationString: (location) => ({ inner: location.slice("wrap+".length) }),
    toProviderInputs: (location) => {
      const { inner } = location as { inner: string };
      return { config: { inner }, target: { kind: "entry", key: inner } };
    },
    async createProvider(config, context: ProviderContext) {
      const { provider: inner } = await context.resolver.createProviderForLocation(
        (config as { inner: string }).inner,
      );
      return {
        ...inner,
        async [Symbol.asyncDispose]() {
          events.push("dispose:wrap");
          await inner[Symbol.asyncDispose]();
        },
      } as IOProvider;
    },
  };
}

describe("ProviderRegistry discovery", () => {
  test("lists protocols, kinds and factories", async () => {
    const fileJs = makeFakeFactory({ protocol: "file" });
    const fileNative = makeFakeFactory({ protocol: "file", kind: PayloadKind.Native });
    const s3 = makeFakeFactory({ protocol: "s3" });
    const converter = makeConverter({ kind: PayloadKind.Native }, { kind: PayloadKind.Js }, 0);
    const registry = await registryOf([fileJs, fileNative, s3], [converter]);
    expect(registry.getProtocols()).toEqual(["file", "s3"]);
    expect(registry.getKinds("file")).toEqual([PayloadKind.Js, PayloadKind.Native]);
    expect(registry.getKinds("none")).toEqual([]);
    expect(registry.getFactory("file")).toBe(fileJs);
    expect(registry.getFactory("file", PayloadKind.Native)).toBe(fileNative);
    expect(registry.getFactory("none")).toBeUndefined();
    expect(registry.getConverters()).toEqual([converter]);
  });

  test("without a js factory, getFactory returns the only one", async () => {
    const native = makeFakeFactory({ protocol: "dev", kind: PayloadKind.Native });
    const registry = await registryOf([native]);
    expect(registry.getFactory("dev")).toBe(native);
  });

  test("rejects two factories for the same (protocol, kind)", async () => {
    const registry = new ProviderRegistry(
      makeFakePluginManager([
        makeFakeFactory({ protocol: "file" }),
        makeFakeFactory({ protocol: "file" }),
      ]),
    );
    await expect(registry.discover()).rejects.toThrow(
      'Factories "factory-0-file-js" and "factory-1-file-js" both provide protocol "file" with payload kind "js"',
    );
  });

  test("rejects same-protocol factories with different location fields", async () => {
    const registry = new ProviderRegistry(
      makeFakePluginManager([
        makeFakeFactory({ protocol: "file" }),
        makeFakeFactory({ protocol: "file", kind: PayloadKind.Native, extraLocationField: "x" }),
      ]),
    );
    await expect(registry.discover()).rejects.toThrow("have different location fields");
  });
});

describe("ProviderRegistry.createProviderForLocation", () => {
  test("prefers js, honours an explicit kind, and defaults the native domain", async () => {
    const contexts: ProviderContext[] = [];
    const registry = await registryOf([
      makeFakeFactory({ protocol: "file", contexts }),
      makeFakeFactory({
        protocol: "file",
        kind: PayloadKind.Native,
        domains: ["host", "gpu"],
        contexts,
      }),
    ]);
    const js = await registry.createProviderForLocation("file://a.txt");
    expect(js.provider.kind).toBe(PayloadKind.Js);
    expect(js.target).toEqual({ kind: "entry", key: "a.txt" });
    expect(contexts[0]?.domain).toBeUndefined();

    const native = await registry.createProviderForLocation("file://a.txt", {
      kind: PayloadKind.Native,
    });
    expect(native.provider.kind).toBe(PayloadKind.Native);
    expect(contexts[1]?.domain).toBe("host");

    await registry.createProviderForLocation("file://a", {
      kind: PayloadKind.Native,
      domain: "gpu",
    });
    expect(contexts[2]?.domain).toBe("gpu");
  });

  test("reports unknown protocols, unavailable kinds and unsupported domains", async () => {
    const registry = await registryOf([
      makeFakeFactory({ protocol: "file" }),
      makeFakeFactory({ protocol: "dev", kind: PayloadKind.Native }),
    ]);
    await expect(registry.createProviderForLocation("s3://b/k")).rejects.toThrow(
      'No provider for protocol "s3" (available: dev, file)',
    );
    await expect(
      registry.createProviderForLocation("file://a", { kind: PayloadKind.Native }),
    ).rejects.toThrow('No provider for protocol "file" with payload kind "native" (available: js)');
    await expect(registry.createProviderForLocation("dev://a", { domain: "gpu" })).rejects.toThrow(
      'No provider for protocol "dev" supports domain "gpu" (available: host)',
    );
  });

  test("a composite provider resolves its inner provider and disposes it with itself", async () => {
    const events: string[] = [];
    const registry = await registryOf([
      makeWrapFactory(events),
      makeFakeFactory({ protocol: "file" }),
    ]);
    const { provider, target } = await registry.createProviderForLocation("wrap+file://a.txt");
    expect(target).toEqual({ kind: "entry", key: "file://a.txt" });
    expect(provider.kind).toBe(PayloadKind.Js);
    await provider[Symbol.asyncDispose]();
    expect(events).toEqual(["dispose:wrap"]);
  });

  test("fails when resolution nests too deeply", async () => {
    const loop: IOProviderFactory = {
      ...makeFakeFactory({ protocol: "loop" }),
      async createProvider(_config, context) {
        return (await context.resolver.createProviderForLocation("loop://again")).provider;
      },
    };
    const registry = await registryOf([loop]);
    const error = await registry.createProviderForLocation("loop://a").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(PermanentIOError);
    expect((error as Error).message).toBe("provider resolution too deep");
    expect(MAX_RESOLUTION_DEPTH).toBe(4);
  });

  test("the inner resolver defaults to the outer kind and domain", async () => {
    const contexts: ProviderContext[] = [];
    const outer: IOProviderFactory = {
      ...makeFakeFactory({ protocol: "outer", kind: PayloadKind.Native, domains: ["gpu"] }),
      async createProvider(_config, context) {
        return (await context.resolver.createProviderForLocation("inner://x")).provider;
      },
    };
    const registry = await registryOf([
      outer,
      makeFakeFactory({
        protocol: "inner",
        kind: PayloadKind.Native,
        domains: ["host", "gpu"],
        contexts,
      }),
    ]);
    await registry.createProviderForLocation("outer://a");
    expect(contexts[0]?.domain).toBe("gpu");
  });
});

describe("ProviderRegistry.createProvidersForTransfer", () => {
  test("negotiates kinds, the converter and the path, and reports the sink's payload types", async () => {
    const converter = makeConverter({ kind: PayloadKind.Native }, { kind: PayloadKind.Js }, 0);
    const registry = await registryOf(
      [
        makeFakeFactory({ protocol: "dev", kind: PayloadKind.Native }),
        makeFakeFactory({ protocol: "s3", writePayloadTypes: ["bytes", "urn:x"] }),
      ],
      [converter],
    );
    const result = await registry.createProvidersForTransfer("dev://a", "s3://b");
    expect(result.source.provider.kind).toBe(PayloadKind.Native);
    expect(result.dest.provider.kind).toBe(PayloadKind.Js);
    expect(result.converter).toBe(converter);
    expect(result.path).toBe("dev/native[host] -> s3/js via native->js (zero-copy)");
    expect(result.writePayloadTypes).toEqual(["bytes", "urn:x"]);
  });

  test("defaults writePayloadTypes to bytes and honours an explicit kind", async () => {
    const registry = await registryOf([
      makeFakeFactory({ protocol: "file" }),
      makeFakeFactory({ protocol: "file", kind: PayloadKind.Native }),
    ]);
    const result = await registry.createProvidersForTransfer("file://a", "file://b", {
      kind: PayloadKind.Native,
    });
    expect(result.path).toBe("file/native[host] -> file/native[host]");
    expect(result.writePayloadTypes).toEqual(["bytes"]);
    await expect(registry.createProvidersForTransfer("file://a", "nope://b")).rejects.toThrow(
      'No provider for protocol "nope"',
    );
  });

  test("disposes the source provider when the destination cannot be created", async () => {
    const events: string[] = [];
    const failing: IOProviderFactory = {
      ...makeFakeFactory({ protocol: "bad" }),
      createProvider: () => Promise.reject(new Error("cannot connect")),
    };
    const source: IOProviderFactory = {
      ...makeFakeFactory({ protocol: "file" }),
      async createProvider() {
        return {
          kind: PayloadKind.Js,
          async [Symbol.asyncDispose]() {
            events.push("dispose:source");
          },
        } as IOProvider;
      },
    };
    const registry = await registryOf([source, failing]);
    await expect(registry.createProvidersForTransfer("file://a", "bad://b")).rejects.toThrow(
      "cannot connect",
    );
    expect(events).toEqual(["dispose:source"]);
  });
});
