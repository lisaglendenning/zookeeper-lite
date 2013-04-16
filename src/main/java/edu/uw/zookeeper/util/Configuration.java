package edu.uw.zookeeper.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

public class Configuration extends ConfigurableReference<Config> {

    public static Configuration create() {
        return new Configuration();
    }

    public static Configuration create(Config config) {
        Factory factory = Factory.create(config);
        return new Configuration(factory);
    }

    protected Configuration() {
        super(Factory.create());
    }
    
    protected Configuration(ConfigurableFactory<Config> configurable) {
        super(configurable);
    }
    
    protected Configuration(ConfigurableFactory<Config> configurable, Config value) {
        super(configurable, value);
    }

    public static class Factory extends AbstractConfigurableFactory<Config> {

        public static String CONFIG_PATH = "edu.uw.zookeeper";
        
        public static Factory create() {
            Config config = ConfigFactory.load();
            try {
                config = config.getConfig(CONFIG_PATH);
            } catch (ConfigException.Missing e) {
                config = ConfigFactory.empty();
            }
            return create(config);
        }

        public static Factory create(Config defaults) {
            return new Factory(defaults);
        }

        protected Factory(Config defaults) {
            super(defaults);
        }
        
        @Override
        public Config get(Config config) {
            if (config != defaults) {
                config = config.withFallback(defaults);
            }
            return config;
        }
    }
}
