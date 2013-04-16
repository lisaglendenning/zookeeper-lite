package edu.uw.zookeeper.util;

import com.typesafe.config.Config;

public interface ConfigurableFactory<T> {
    public T get();
    public T get(Config configuration);
}
