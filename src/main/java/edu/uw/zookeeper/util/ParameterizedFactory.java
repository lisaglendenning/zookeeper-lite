package edu.uw.zookeeper.util;

public interface ParameterizedFactory<V,T> {
    T get(V value);
}
