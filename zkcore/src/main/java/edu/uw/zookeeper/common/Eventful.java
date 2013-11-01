package edu.uw.zookeeper.common;

public interface Eventful<T> {
    void subscribe(T listener);
    boolean unsubscribe(T listener);
}
