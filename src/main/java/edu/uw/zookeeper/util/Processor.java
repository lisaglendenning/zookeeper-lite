package edu.uw.zookeeper.util;

public interface Processor<V,T> {
    T apply(V input) throws Exception;
}
