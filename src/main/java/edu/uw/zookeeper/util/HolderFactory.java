package edu.uw.zookeeper.util;

import edu.uw.zookeeper.util.Factory;

public class HolderFactory<T> implements Singleton<T> {

    public static <T> HolderFactory<T> newInstance(Factory<T> delegate) {
        return new HolderFactory<T>(delegate.get());
    }
    
    public static <T> HolderFactory<T> newInstance(T value) {
        return new HolderFactory<T>(value);
    }
    
    private final T value;
    
    protected HolderFactory(T value) {
        this.value = value;
    }
    
    @Override
    public T get() {
        return value;
    }
}
