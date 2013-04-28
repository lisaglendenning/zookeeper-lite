package edu.uw.zookeeper.util;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Configuration {

    public static class DefaultConfigFactory implements Factory<Config> {
        
        public static enum Singleton implements Factory<Config> {
            INSTANCE;

            public static Singleton getInstance() {
                return INSTANCE;
            }
            
            private final DefaultConfigFactory instance;
            
            private Singleton() {
                this.instance = DefaultConfigFactory.create();
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

    private Arguments arguments;
    private Config config;
    
    @Inject
    private Configuration(Arguments arguments, Config config) {
        this.arguments = arguments;
        this.config = config;
    }
    
    public Arguments asArguments() {
        return arguments;
    }
    
    public Config asConfig() {
        return config;
    }
    
    public Config add(Config updated) {
        config = updated.withFallback(config);
        return config;
    }
}
