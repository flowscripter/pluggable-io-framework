# Plugins

Plugin support is provided via
[dynamic-plugin-framework](https://github.com/flowscripter/dynamic-plugin-framework).

## Extension points

| Extension point constant                                   | Extension type              |
| ---------------------------------------------------------- | --------------------------- |
| `PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT`  | `IOProviderFactory`         |
| `PLUGGABLE_IO_FRAMEWORK_PAYLOAD_CONVERTER_EXTENSION_POINT` | `PayloadConverterExtension` |

## Writing a plugin

Plugin authors should depend on
[`@flowscripter/pluggable-io-framework-api`](https://github.com/flowscripter/pluggable-io-framework-api)
rather than this package, as a `peerDependency`:

```json
{
  "peerDependencies": {
    "@flowscripter/pluggable-io-framework-api": "*"
  }
}
```

A provider plugin registers an `IOProviderFactory` that declares its
`protocol`, payload `kind`, location, config and property schemas, and
creates providers. Factories for the same protocol must use the same
top-level location fields. A converter plugin registers
`PayloadConverterExtension`s describing the kinds and domains they convert
between and whether they copy.

See [io-plugin-filesystem](https://github.com/flowscripter/io-plugin-filesystem)
for a reference provider plugin.

## Discovery

`ProviderRegistry.discover()` registers both extension points with the
`PluginManager`, instantiates every factory and converter once, and rejects
two factories for the same (protocol, kind) pair.
