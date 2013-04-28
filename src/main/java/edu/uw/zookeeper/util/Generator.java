package edu.uw.zookeeper.util;

public interface Generator<T> extends Reference<T> {
    public T next();
}
