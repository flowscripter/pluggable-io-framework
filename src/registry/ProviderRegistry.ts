import type { PluginManager } from "@flowscripter/dynamic-plugin-framework";
import {
  BYTES_PAYLOAD_TYPE,
  type IOProvider,
  type IOProviderFactory,
  type LocationTarget,
  PayloadKind,
  type PayloadConverter,
  PermanentIOError,
  PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT,
  PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
  type ProviderResolver,
} from "@flowscripter/pluggable-io-framework-api";
import type { TransferOptions } from "../transfer/TransferOptions.ts";
import { detectProtocol } from "./detectProtocol.ts";
import type { StructuredLocation } from "./StructuredLocation.ts";
import { DEFAULT_NATIVE_DOMAINS, negotiateTransfer } from "./negotiateTransfer.ts";

/** Nested provider resolutions allowed before resolution fails. */
export const MAX_RESOLUTION_DEPTH = 4;

export interface ResolvedProvider {
  readonly provider: IOProvider;
  readonly target: LocationTarget;
}

export interface TransferProviders {
  readonly source: ResolvedProvider;
  readonly dest: ResolvedProvider;
  /**
   * The negotiated `converter`, `path` and `writePayloadTypes`, to pass to
   * `copy`/`move` as is or spread together with caller options.
   */
  readonly options: TransferOptions;
}

function protocolOf(location: string | StructuredLocation): string {
  return typeof location === "string" ? detectProtocol(location) : location.protocol;
}

function locationFieldNames(factory: IOProviderFactory): string[] {
  const shape = (factory.locationSchema as { shape?: Record<string, unknown> }).shape;
  return Object.keys(shape ?? {}).sort();
}

/**
 * Discovers provider factories and payload converters through a
 * `dynamic-plugin-framework` `PluginManager`, keyed by protocol and
 * payload kind, and creates providers for location strings or
 * {@link StructuredLocation}s. It is also the
 * `ProviderResolver` handed to every provider it creates.
 */
export class ProviderRegistry implements ProviderResolver {
  #factories = new Map<string, Map<PayloadKind, IOProviderFactory>>();
  #converters: PayloadConverter[] = [];

  public constructor(private readonly pluginManager: PluginManager) {}

  /**
   * Instantiates every registered factory and converter once. Fails, naming
   * both factories, if two share a (protocol, kind) pair or if factories for
   * the same protocol have different top-level location fields.
   */
  public async discover(): Promise<void> {
    const factories = new Map<string, Map<PayloadKind, IOProviderFactory>>();
    const handles = new Map<IOProviderFactory, string>();
    for (const extension of await this.#extensions(
      PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
    )) {
      const factory = (await this.pluginManager.instantiate(
        extension.extensionHandle,
      )) as IOProviderFactory;
      handles.set(factory, extension.extensionHandle);
      const byKind = factories.get(factory.protocol) ?? new Map<PayloadKind, IOProviderFactory>();
      const clash = byKind.get(factory.kind);
      if (clash) {
        throw new Error(
          `Factories "${handles.get(clash)}" and "${extension.extensionHandle}" both provide protocol "${factory.protocol}" with payload kind "${factory.kind}"`,
        );
      }
      for (const other of byKind.values()) {
        if (locationFieldNames(other).join() !== locationFieldNames(factory).join()) {
          throw new Error(
            `Factories "${handles.get(other)}" and "${extension.extensionHandle}" for protocol "${factory.protocol}" have different location fields`,
          );
        }
      }
      byKind.set(factory.kind, factory);
      factories.set(factory.protocol, byKind);
    }
    const converters: PayloadConverter[] = [];
    for (const extension of await this.#extensions(
      PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT,
    )) {
      converters.push(
        (await this.pluginManager.instantiate(extension.extensionHandle)) as PayloadConverter,
      );
    }
    this.#factories = factories;
    this.#converters = converters;
  }

  public getProtocols(): string[] {
    return [...this.#factories.keys()].sort();
  }

  public getKinds(protocol: string): PayloadKind[] {
    return [...(this.#factories.get(protocol)?.keys() ?? [])].sort();
  }

  /** The factory for `protocol` and `kind`; without `kind`, the `js` one, else the only one. */
  public getFactory(protocol: string, kind?: PayloadKind): IOProviderFactory | undefined {
    const byKind = this.#factories.get(protocol);
    if (!byKind) return undefined;
    if (kind !== undefined) return byKind.get(kind);
    return byKind.get(PayloadKind.Js) ?? byKind.values().next().value;
  }

  public getConverters(): readonly PayloadConverter[] {
    return this.#converters;
  }

  public createProviderForLocation(
    location: string | StructuredLocation,
    options?: { kind?: PayloadKind; domain?: string },
  ): Promise<ResolvedProvider> {
    return this.#resolve(location, options, 0);
  }

  /**
   * Creates providers for both sides of a transfer, negotiating a common
   * (kind, domain) and payload type, or a converter.
   */
  public async createProvidersForTransfer(
    source: string | StructuredLocation,
    dest: string | StructuredLocation,
    options?: { kind?: PayloadKind },
  ): Promise<TransferProviders> {
    const sourceProtocol = protocolOf(source);
    const destProtocol = protocolOf(dest);
    const sourceFactories = this.#factoriesFor(sourceProtocol, options?.kind);
    const destFactories = this.#factoriesFor(destProtocol, options?.kind);
    const negotiation = negotiateTransfer(
      sourceProtocol,
      sourceFactories,
      destProtocol,
      destFactories,
      this.#converters,
      options?.kind,
    );
    const sourceResolved = await this.#instantiate(
      negotiation.source.factory,
      source,
      negotiation.source.domain,
      0,
    );
    let destResolved: ResolvedProvider;
    try {
      destResolved = await this.#instantiate(
        negotiation.dest.factory,
        dest,
        negotiation.dest.domain,
        0,
      );
    } catch (error) {
      await sourceResolved.provider[Symbol.asyncDispose]();
      throw error;
    }
    return {
      source: sourceResolved,
      dest: destResolved,
      options: {
        converter: negotiation.converter,
        path: negotiation.path,
        writePayloadTypes: negotiation.dest.factory.writePayloadTypes ?? [BYTES_PAYLOAD_TYPE],
      },
    };
  }

  #extensions(extensionPoint: string) {
    return this.pluginManager
      .registerExtensions(extensionPoint)
      .then(() => this.pluginManager.getRegisteredExtensions(extensionPoint));
  }

  #factoriesFor(protocol: string, kind?: PayloadKind): IOProviderFactory[] {
    const byKind = this.#factories.get(protocol);
    if (!byKind) {
      throw new Error(
        `No provider for protocol "${protocol}" (available: ${this.getProtocols().join(", ")})`,
      );
    }
    if (kind !== undefined && !byKind.has(kind)) {
      throw new Error(
        `No provider for protocol "${protocol}" with payload kind "${kind}" (available: ${this.getKinds(protocol).join(", ")})`,
      );
    }
    return [...byKind.values()];
  }

  async #resolve(
    location: string | StructuredLocation,
    options: { kind?: PayloadKind; domain?: string } | undefined,
    depth: number,
  ): Promise<ResolvedProvider> {
    if (depth > MAX_RESOLUTION_DEPTH) {
      throw new PermanentIOError("provider resolution too deep");
    }
    const protocol = protocolOf(location);
    this.#factoriesFor(protocol, options?.kind);
    const factory = this.getFactory(protocol, options?.kind) as IOProviderFactory;
    let domain: string | undefined;
    if (factory.kind === PayloadKind.Native) {
      const domains = factory.domains ?? DEFAULT_NATIVE_DOMAINS;
      domain = options?.domain ?? domains[0];
      if (domain === undefined || !domains.includes(domain)) {
        throw new Error(
          `No provider for protocol "${protocol}" supports domain "${domain}" (available: ${domains.join(", ")})`,
        );
      }
    }
    return this.#instantiate(factory, location, domain, depth);
  }

  async #instantiate(
    factory: IOProviderFactory,
    location: string | StructuredLocation,
    domain: string | undefined,
    depth: number,
  ): Promise<ResolvedProvider> {
    const raw =
      typeof location === "string" ? factory.parseLocationString(location) : location.location;
    const parsed = factory.locationSchema.parse(raw);
    const { config, target } = factory.toProviderInputs(parsed);
    const resolver: ProviderResolver = {
      createProviderForLocation: (inner, opts) =>
        this.#resolve(
          inner,
          { kind: opts?.kind ?? factory.kind, domain: opts?.domain ?? domain },
          depth + 1,
        ),
    };
    const provider = await factory.createProvider(factory.configSchema.parse(config), {
      domain,
      resolver,
    });
    return { provider, target };
  }
}
