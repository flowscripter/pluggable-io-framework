import {
  BYTES_PAYLOAD_TYPE,
  type IOProviderFactory,
  type PayloadConverter,
  PayloadKind,
} from "@flowscripter/pluggable-io-framework-api";
import { describeConverter, describeKind } from "../util/describePath.ts";

/** One (factory, kind, domain) choice for one side of a transfer. */
export interface Endpoint {
  readonly factory: IOProviderFactory;
  readonly kind: PayloadKind;
  readonly domain?: string;
}

export interface Negotiation {
  readonly source: Endpoint;
  readonly dest: Endpoint;
  readonly converter?: PayloadConverter;
  readonly path: string;
}

export const DEFAULT_NATIVE_DOMAINS: readonly string[] = ["host"];

/** Every (kind, domain) a factory offers, in its preference order. */
export function endpointsOf(factory: IOProviderFactory): Endpoint[] {
  if (factory.kind === PayloadKind.Js) {
    return [{ factory, kind: factory.kind }];
  }
  return (factory.domains ?? DEFAULT_NATIVE_DOMAINS).map((domain) => ({
    factory,
    kind: factory.kind,
    domain,
  }));
}

function sharesPayloadType(source: Endpoint, dest: Endpoint): boolean {
  const reads = source.factory.readPayloadTypes ?? [BYTES_PAYLOAD_TYPE];
  const writes = dest.factory.writePayloadTypes ?? [BYTES_PAYLOAD_TYPE];
  return reads.some((payloadType) => writes.includes(payloadType));
}

function matches(end: PayloadConverter["from"], endpoint: Endpoint): boolean {
  return end.kind === endpoint.kind && (end.domain === undefined || end.domain === endpoint.domain);
}

function kindOrder(kind: PayloadKind): number {
  return kind === PayloadKind.Js ? 0 : 1;
}

function describeSide(protocol: string, endpoints: readonly Endpoint[]): string {
  const kinds = endpoints.map((endpoint) => describeKind(endpoint.kind, endpoint.domain));
  return `"${protocol}" supports [${kinds.join(", ")}]`;
}

/**
 * Chooses the (kind, domain) for each side of a transfer between factories
 * of two protocols, considering only pairs that share an exact payload type:
 * 1. a common (kind, domain), preferring `js`, then the source factory's
 *    domain order;
 * 2. otherwise the cheapest single converter from a source pair to a
 *    destination pair;
 * 3. otherwise an error listing what each side supports.
 * An explicit `kind` restricts both sides to it, and allows only converters
 * that keep it.
 */
export function negotiateTransfer(
  sourceProtocol: string,
  sourceFactories: readonly IOProviderFactory[],
  destProtocol: string,
  destFactories: readonly IOProviderFactory[],
  converters: readonly PayloadConverter[],
  kind?: PayloadKind,
): Negotiation {
  const restrict = (factories: readonly IOProviderFactory[]) =>
    factories
      .filter((factory) => kind === undefined || factory.kind === kind)
      .flatMap(endpointsOf)
      .sort((a, b) => kindOrder(a.kind) - kindOrder(b.kind));
  const sources = restrict(sourceFactories);
  const dests = restrict(destFactories);
  const describe = (source: Endpoint, dest: Endpoint, converter?: PayloadConverter) =>
    `${sourceProtocol}/${describeKind(source.kind, source.domain)} -> ${destProtocol}/${describeKind(dest.kind, dest.domain)}${converter ? describeConverter(converter) : ""}`;

  const pairs = sources.flatMap((source) =>
    dests.filter((dest) => sharesPayloadType(source, dest)).map((dest) => ({ source, dest })),
  );
  if (pairs.length === 0 && sources.length > 0 && dests.length > 0) {
    const reads = sourceFactories.flatMap((f) => f.readPayloadTypes ?? [BYTES_PAYLOAD_TYPE]);
    const writes = destFactories.flatMap((f) => f.writePayloadTypes ?? [BYTES_PAYLOAD_TYPE]);
    throw new Error(
      `No common payload type: "${sourceProtocol}" reads [${[...new Set(reads)].join(", ")}], "${destProtocol}" writes [${[...new Set(writes)].join(", ")}]`,
    );
  }

  const common = pairs.find(
    ({ source, dest }) => source.kind === dest.kind && source.domain === dest.domain,
  );
  if (common) {
    return { ...common, path: describe(common.source, common.dest) };
  }

  const usable = converters
    .filter(
      (converter) =>
        kind === undefined || (converter.from.kind === kind && converter.to.kind === kind),
    )
    .toSorted((a, b) => a.cost - b.cost);
  for (const converter of usable) {
    const pair = pairs.find(
      ({ source, dest }) => matches(converter.from, source) && matches(converter.to, dest),
    );
    if (pair) {
      return { ...pair, converter, path: describe(pair.source, pair.dest, converter) };
    }
  }

  throw new Error(
    `No common payload kind/domain and no registered converter between ${describeSide(sourceProtocol, sources)} and ${describeSide(destProtocol, dests)}`,
  );
}
