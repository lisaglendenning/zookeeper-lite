package org.apache.zookeeper.util;

public interface AtomicUpdater<T> {

    T get();

    boolean set(T nextValue);

    T getAndSet(T nextValue);

    boolean compareAndSet(T prevValue, T nextValue);
}
