import type {
  PayloadConverterExtension,
  PayloadKind,
} from "@flowscripter/pluggable-io-framework-api";

/** `kind` or `kind[domain]`. */
export function describeKind(kind: PayloadKind, domain?: string): string {
  return domain === undefined ? kind : `${kind}[${domain}]`;
}

/** ` via from->to (zero-copy)` or ` via from->to (copy)`. */
export function describeConverter(converter: PayloadConverterExtension): string {
  const cost = converter.cost === 0 ? "zero-copy" : "copy";
  return ` via ${describeKind(converter.from.kind, converter.from.domain)}->${describeKind(converter.to.kind, converter.to.domain)} (${cost})`;
}
