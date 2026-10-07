/**
 * A location given as its protocol plus the raw location object that the
 * protocol's factory `locationSchema` validates, instead of a location
 * string. It can carry fields a string cannot, such as `filename`,
 * `pattern` or credentials.
 */
export interface StructuredLocation {
  readonly protocol: string;
  readonly location: unknown;
}
