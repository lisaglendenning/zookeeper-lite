package edu.uw.zookeeper.util;

import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

public abstract class AbstractConfigurableFactory<T> implements ConfigurableFactory<T> {

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
        return get(defaults());
    }
}
