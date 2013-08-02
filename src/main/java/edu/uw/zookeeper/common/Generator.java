package edu.uw.zookeeper.common;

public interface Generator<T> extends Reference<T> {
    public T next();
}
