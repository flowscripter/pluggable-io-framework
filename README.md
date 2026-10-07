# pluggable-io-framework

[![version](https://img.shields.io/github/v/release/flowscripter/pluggable-io-framework?sort=semver)](https://github.com/flowscripter/pluggable-io-framework/releases)
[![build](https://img.shields.io/github/actions/workflow/status/flowscripter/pluggable-io-framework/release-bun-library.yml)](https://github.com/flowscripter/pluggable-io-framework/actions/workflows/release-bun-library.yml)
[![docs](https://img.shields.io/badge/docs-API-blue)](https://flowscripter.github.io/pluggable-io-framework/index.html)
[![license: MIT](https://img.shields.io/github/license/flowscripter/pluggable-io-framework)](https://github.com/flowscripter/pluggable-io-framework/blob/main/LICENSE)

> A pluggable source/sink IO framework using
> [dynamic-plugin-framework](https://github.com/flowscripter/dynamic-plugin-framework)

## Key Features

- Protocol-aware locations, given as a string (`file:///data/a.txt`,
  `s3://bucket/key`, `https://host/a`, composite schemes such as
  `tams+https:`; bare paths default to `file`) or as a structured
  `{ protocol, location }` object. Either form selects the installed provider
  plugin for that protocol. See
  [String and Structured Locations](README/key-concepts.md#string-and-structured-locations).
- A `ProviderRegistry` keyed by protocol and payload kind, with more than one
  implementation of a protocol installed side by side (e.g. JS and native
  `file` providers).
- Negotiation of the payload kind, memory domain and payload type between
  source and sink, falling back to the cheapest registered payload converter
  plugin.
- `copy`/`move` over explicit entry, container and glob pattern targets,
  with `cp -r` semantics for containers.
- Direct provider-to-provider transfers, multipart transfers with negotiated
  part sizes, zero-copy lease transfers into sink-provided buffers, or plain
  streaming - chosen automatically.
- Graceful stop and cancellation, per-part retries, resumable writes and
  automatic reconnection of live sources with gap reporting.
- Concurrency-bounded recursive and pattern transfers with progress
  telemetry.
- Provider composition: a provider can resolve other installed providers.
- Stream decorators: `seekable` and `locallyCached`.
- Bun based, written in TypeScript, based on native JavaScript modules.

## Usage Examples

- [flowscripter-io-cli](https://github.com/flowscripter/flowscripter-io-cli)
  is a CLI application based on this framework.
- [io-plugin-filesystem](https://github.com/flowscripter/io-plugin-filesystem)
  is the reference `file` provider plugin.

```typescript
import {
  DefaultPluginManager,
  LocalFolderPluginRepository,
} from "@flowscripter/dynamic-plugin-framework";
import { copy, ProviderRegistry } from "@flowscripter/pluggable-io-framework";

const registry = new ProviderRegistry(
  new DefaultPluginManager([new LocalFolderPluginRepository("./plugins")]),
);
await registry.discover();

const { source, dest, options } = await registry.createProvidersForTransfer(
  "file:///data/in",
  "s3://bucket/out",
);

const result = await copy(source.provider, source.target, dest.provider, dest.target, {
  ...options,
  telemetry: { onProgress: (event) => console.log(event) },
});
console.log(result.path);
```

## Further Details

- [Key Concepts](./README/key-concepts.md)
- [Implementation Details](./README/implementation-details.md)
- [Plugins](./README/plugins.md)
- [Development](./README/development.md)
- [API Documentation](https://flowscripter.github.io/pluggable-io-framework/index.html)

## License

MIT © Flowscripter
