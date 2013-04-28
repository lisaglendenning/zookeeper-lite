package edu.uw.zookeeper.util;

import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

public abstract class AbstractConfigurableFactory<T> implements DefaultsFactory<Config, T> {

    private final Config defaults;

    protected AbstractConfigurableFactory(Map<String, Object> defaults) {
        this(ConfigValueFactory.fromMap(defaults).toConfig());
    }
    
    protected AbstractConfigurableFactory(Config defaults) {
        this.defaults = defaults;
    }
    
    protected Config defaults() {
        return defaults;
    }
        
    @Override
    public T get() {
        return fromConfig(defaults());
    }
    
    @Override
    public T get(Config config) {
        return fromConfig(config.withFallback(defaults()));
    }
    
    protected abstract T fromConfig(Config config);
}
