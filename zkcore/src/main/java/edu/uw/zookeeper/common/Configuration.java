package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValueFactory;

public class Configuration {

    protected static String CONFIG_PATH = "edu.uw.zookeeper";
    
    public static String getConfigPath() {
        return CONFIG_PATH;
    }

    protected static Config loadDefaults() {
        Config config = ConfigFactory.load();
        config = config.hasPath(CONFIG_PATH) ?
                config.getConfig(CONFIG_PATH) : ConfigFactory.empty();
        return config;
    }
    
    public static Config defaultConfig() {
        return DefaultConfig.DEFAULT.get();
    }
    
    protected static enum DefaultConfig implements Singleton<Config> {
        DEFAULT(loadDefaults());

        private final Config instance;

        private DefaultConfig(Config instance) {
            this.instance = instance;
        }
        
        @Override
        public Config get() {
            return instance;
        }
    }
    
    public static Configuration defaults(Arguments arguments) {
        return create(arguments, DefaultConfig.DEFAULT.get());
    }
    
    public static Configuration createEmpty() {
        return create(SimpleArguments.defaults(), ConfigFactory.empty());
    }
    
    public static Configuration fromConfiguration(Configuration configuration) {
        return create(configuration.getArguments(), configuration.getConfig());
    }

    public static Configuration create(Arguments arguments, Config config) {
        return new Configuration(arguments, config);
    }

    private final Arguments arguments;
    private Config config;
    
    protected Configuration(Arguments arguments, Config config) {
        this.arguments = arguments;
        this.config = config;
    }
    
    public Arguments getArguments() {
        return arguments;
    }
    
    public synchronized Config getConfig() {
        return config;
    }
    
    public synchronized Configuration withConfig(Config updates) {
        this.config = updates.withFallback(this.config);
        return this;
    }

    public synchronized Config getConfigOrEmpty(String path) {
        Config config = this.config;
        if (! path.isEmpty()) {
            if (config.hasPath(path)) {
                config = config.getConfig(path);
            } else {
                config = ConfigFactory.empty();
            }
        }
        return config;
    }
    
    public synchronized Configuration withConfigurable(Configurable configurable) {
        String key = configurable.key();
        String arg = configurable.arg();
        if (key.isEmpty()) {
            key = arg;
        }
        checkArgument(!key.isEmpty(), configurable.toString());
        if (! configurable.value().isEmpty()) {
            String path = configurable.path().isEmpty() ? key : ConfigUtil.joinPath(configurable.path(), key);
            if (! config.hasPath(path)) {
                Config update = ConfigValueFactory.fromAnyRef(configurable.value()).atKey(key);
                if (! configurable.path().isEmpty()) {
                    update = update.atPath(configurable.path());
                }
                withConfig(update);
            }
        }
        if (! arg.isEmpty()) {
            Optional<String> help = Optional.of(
                    configurable.help().isEmpty() ? configurable.type().toString() : configurable.help());
            Optional<String> defaultValue = Optional.fromNullable(
                    configurable.value().isEmpty() ? null : configurable.value());
            arguments.addOption(arg, help, defaultValue);
            arguments.parse();
            if (arguments.hasValue(arg)) {
                String value = arguments.getValue(arg);
                if (! defaultValue.isPresent() || ! defaultValue.get().equals(value)) {
                    Config update = ConfigValueFactory.fromAnyRef(value).atKey(key);
                    if (! configurable.path().isEmpty()) {
                        update = update.atPath(configurable.path());
                    }
                    withConfig(update);
                }
            }
        }
        return this;
    }
    
    @Override
    public synchronized String toString() {
        return Objects.toStringHelper(this)
                .add("config", config)
                .add("arguments", arguments).toString();
    }
}
