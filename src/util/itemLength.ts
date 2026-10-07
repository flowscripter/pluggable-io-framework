import { type Item, PayloadKind } from "@flowscripter/pluggable-io-framework-api";

/** The payload size of an item in bytes. */
export function itemLength(item: Item): number {
  return item.payload.kind === PayloadKind.Js ? item.payload.data.byteLength : item.payload.length;
}
