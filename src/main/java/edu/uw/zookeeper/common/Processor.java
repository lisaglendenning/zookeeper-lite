package edu.uw.zookeeper.common;

public interface Processor<V,T> {
    T apply(V input) throws Exception;
}
