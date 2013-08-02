package edu.uw.zookeeper.common;

public interface ParameterizedFactory<V,T> {
    T get(V value);
}
