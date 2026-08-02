import type { PluginManager } from "@flowscripter/dynamic-plugin-framework";
import {
  PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
  type IOProvider,
  type IOProviderFactory,
} from "@flowscripter/pluggable-io-framework-api";

/**
 * Wraps a `dynamic-plugin-framework` {@link PluginManager} to discover and
 * instantiate pluggable-io-framework source/sink provider plugins.
 */
export class ProviderRegistry {
  public constructor(private readonly pluginManager: PluginManager) {}

  public async discover(): Promise<void> {
    await this.pluginManager.registerExtensions(
      PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
    );
  }

  public async listAvailableProviders() {
    return this.pluginManager.getRegisteredExtensions(
      PLUGGABLE_IO_FRAMEWORK_PROVIDER_FACTORY_EXTENSION_POINT,
    );
  }

  public async createProvider(extensionHandle: string, config: unknown): Promise<IOProvider> {
    const factory = (await this.pluginManager.instantiate(extensionHandle)) as IOProviderFactory;
    const validatedConfig = factory.configSchema.parse(config);
    return factory.createProvider(validatedConfig);
  }
}
