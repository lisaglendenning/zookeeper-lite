package org.apache.zookeeper.util;

import com.typesafe.config.Config;
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

        public static Factory create() {
            return new Factory();
        }

        public static Factory create(Config defaults) {
            return new Factory(defaults);
        }

        protected Factory() {
            this(ConfigFactory.load());
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
