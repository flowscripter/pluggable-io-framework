/**
 * Translates a glob into an anchored regular expression matched against a
 * single entry name: `*` matches any run of characters except `/`, `?`
 * matches one such character, and `[...]` is a character class (`[!...]`
 * negates it). Everything else matches literally.
 */
export function globToRegex(glob: string): RegExp {
  let source = "";
  for (let i = 0; i < glob.length; i += 1) {
    const char = glob[i] as string;
    if (char === "*") {
      source += "[^/]*";
    } else if (char === "?") {
      source += "[^/]";
    } else if (char === "[") {
      const close = glob.indexOf("]", i + 2);
      if (close === -1) {
        source += "\\[";
        continue;
      }
      let body = glob.slice(i + 1, close);
      if (body.startsWith("!")) {
        body = `^${body.slice(1)}`;
      }
      source += `[${body.replace(/\\/g, "\\\\")}]`;
      i = close;
    } else {
      source += char.replace(/[.+^${}()|\\/\]]/g, "\\$&");
    }
  }
  return new RegExp(`^${source}$`);
}
