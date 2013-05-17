package edu.uw.zookeeper.util;

import java.util.Map;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class Configuration {

    public static class DefaultConfigFactory implements Singleton<Config> {
        
        public static enum Holder implements Singleton<Config> {
            DEFAULT(DefaultConfigFactory.create());

            public static Holder getInstance() {
                return DEFAULT;
            }
            
            private final DefaultConfigFactory instance;
            
            private Holder(DefaultConfigFactory instance) {
                this.instance = instance;
            }
            
            @Override
            public Config get() {
                return instance.get();
            }
        }
    
        private static String CONFIG_PATH = "edu.uw.zookeeper";
        
        public static String getConfigPath() {
            return CONFIG_PATH;
        }

        public static Config load() {
            Config config = ConfigFactory.load();
            config = config.hasPath(getConfigPath()) ?
                    config.getConfig(getConfigPath()) : ConfigFactory.empty();
            return config;
        }
        
        public static DefaultConfigFactory create() {
            return create(load());
        }
    
        public static DefaultConfigFactory create(Config defaults) {
            return new DefaultConfigFactory(defaults);
        }
    
        private final Config defaults;
        
        private DefaultConfigFactory(Config defaults) {
            this.defaults = defaults;
        }
        
        @Override
        public Config get() {
            return defaults;
        }
    }

    public static Configuration create(Arguments arguments, Config config) {
        return new Configuration(arguments, config);
    }

    private final Arguments arguments;
    private volatile Config config;
    
    protected Configuration(Arguments arguments, Config config) {
        this.arguments = arguments;
        this.config = config;
    }
    
    public Arguments asArguments() {
        return arguments;
    }
    
    public Config asConfig() {
        return config;
    }
    
    public Config updateConfig(Config updated) {
        config = updated.withFallback(config);
        return config;
    }
    
    public Config getConfigOrEmpty(String configPath) {
        Config config = asConfig();
        if (configPath.length() > 0 && config.hasPath(configPath)) {
            config = config.getConfig(configPath);
        } else {
            config = ConfigFactory.empty();
        }
        return config;
    }
    
    public Config withArguments(String configPath, Map.Entry<String, String>...argToConfig) {
        Arguments arguments = asArguments();
        Map<String, Object> args = Maps.newHashMap();
        for (Map.Entry<String, String> entry: argToConfig) {
            if (arguments.hasValue(entry.getKey())) {
                args.put(entry.getValue(), arguments.getValue(entry.getKey()));
            }
        }

        Config config = getConfigOrEmpty(configPath);
        if (! args.isEmpty()) {
            config = ConfigValueFactory.fromMap(args).toConfig().withFallback(config);
        }
        return config;
    }
}
