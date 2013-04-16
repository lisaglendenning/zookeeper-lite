package edu.uw.zookeeper.util;

import java.util.concurrent.atomic.AtomicReference;

import com.typesafe.config.Config;

public class ConfigurableReference<T> implements ConfigurableFactory<T> {

    public static <T> ConfigurableReference<T> create(ConfigurableFactory<T> configurable) {
        return new ConfigurableReference<T>(configurable);
    }

    public static <T> ConfigurableReference<T> create(ConfigurableFactory<T> configurable, Config config) {
        return new ConfigurableReference<T>(configurable, configurable.get(config));
    }
    
    public static <T> ConfigurableReference<T> create(ConfigurableFactory<T> configurable, T value) {
        return new ConfigurableReference<T>(configurable, value);
    }
    
    protected final ConfigurableFactory<T> configurable;
    protected final AtomicReference<T> value;

    protected ConfigurableReference(ConfigurableFactory<T> configurable) {
        this(configurable, configurable.get());
    }

    protected ConfigurableReference(ConfigurableFactory<T> configurable, T value) {
        this.configurable = configurable;
        this.value = new AtomicReference<T>(value);
    }
    
    @Override
    public T get() {
        return value.get();
    }

    @Override
    public T get(Config configuration) {
        T value = configurable.get(configuration);
        this.value.set(value);
        return value;
    }
}
