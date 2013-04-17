package edu.uw.zookeeper.util;

public interface Processor<T, V> {

    V apply(T input) throws Exception;
}