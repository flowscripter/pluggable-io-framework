import type { ExtensionInfo, PluginManager } from "@flowscripter/dynamic-plugin-framework";
import {
  type IOProvider,
  type IOProviderFactory,
  type LocationTarget,
  PayloadKind,
  PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT,
  PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
  type PayloadConverter,
  type ProviderContext,
} from "@flowscripter/pluggable-io-framework-api";
import { z } from "zod";
import { makeMemoryProvider, makeStore } from "./memoryProvider.ts";

export interface FakeFactoryOptions {
  readonly protocol: string;
  readonly kind?: PayloadKind;
  readonly domains?: readonly string[];
  readonly readPayloadTypes?: readonly string[];
  readonly writePayloadTypes?: readonly string[];
  readonly extraLocationField?: string;
  /** Records each `createProvider` context. */
  readonly contexts?: ProviderContext[];
}

/** A factory whose locations are `<protocol>://<key>` and whose providers are in-memory. */
export function makeFakeFactory(options: FakeFactoryOptions): IOProviderFactory {
  const kind = options.kind ?? PayloadKind.Js;
  const shape: Record<string, z.ZodType> = { key: z.string() };
  if (options.extraLocationField) shape[options.extraLocationField] = z.string().optional();
  const locationSchema = z.object(shape);
  return {
    protocol: options.protocol,
    kind,
    domains: options.domains,
    readPayloadTypes: options.readPayloadTypes,
    writePayloadTypes: options.writePayloadTypes,
    configSchema: z.object({ id: z.string() }),
    locationSchema,
    propertySchema: z.object({}),
    settablePropertySchema: z.object({}),
    parseLocationString: (location) => ({ key: location.replace(/^[^:]+:\/\//, "") }),
    toProviderInputs: (location) => {
      const { key } = location as { key: string };
      const target: LocationTarget = { kind: "entry", key };
      return { config: { id: `${options.protocol}/${kind}` }, target };
    },
    async createProvider(config, context) {
      options.contexts?.push(context);
      const provider = makeMemoryProvider(makeStore(), { id: (config as { id: string }).id });
      return { ...provider, kind } as IOProvider;
    },
  };
}

/** A minimal plugin manager serving the given factories and converters. */
export function makeFakePluginManager(
  factories: readonly IOProviderFactory[],
  converters: readonly PayloadConverter[] = [],
): PluginManager {
  const extensions = new Map<string, unknown>();
  const byPoint = new Map<string, ExtensionInfo[]>([
    [PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT, []],
    [PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT, []],
  ]);
  factories.forEach((factory, index) => {
    const handle = `factory-${index}-${factory.protocol}-${factory.kind}`;
    extensions.set(handle, factory);
    byPoint
      .get(PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT)
      ?.push({ extensionHandle: handle });
  });
  converters.forEach((converter, index) => {
    const handle = `converter-${index}`;
    extensions.set(handle, converter);
    byPoint
      .get(PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT)
      ?.push({ extensionHandle: handle });
  });
  return {
    registerExtensions: async () => {},
    getRegisteredExtensions: async (extensionPoint: string) => byPoint.get(extensionPoint) ?? [],
    instantiate: async (handle: string) => extensions.get(handle),
  } as unknown as PluginManager;
}

export function makeConverter(
  from: PayloadConverter["from"],
  to: PayloadConverter["to"],
  cost: 0 | 1,
): PayloadConverter {
  return { from, to, cost, convert: (item) => item };
}
