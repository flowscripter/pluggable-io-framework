const SCHEME = /^([a-z][a-z0-9+.-]+):/i;

/**
 * Returns the protocol of a location string: its URI scheme, lower-cased,
 * or `"file"` when there is none. A scheme must be at least two characters,
 * so a Windows drive letter (`C:\foo`) is not read as one. For a composite
 * scheme the protocol is the part before the first `+`.
 */
export function detectProtocol(input: string): string {
  const match = SCHEME.exec(input);
  if (!match) {
    return "file";
  }
  const scheme = (match[1] as string).toLowerCase();
  return scheme.split("+")[0] as string;
}
