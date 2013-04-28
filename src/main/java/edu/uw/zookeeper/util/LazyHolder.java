package edu.uw.zookeeper.util;

public class LazyHolder<T> implements Singleton<T> {

    public static <T> LazyHolder<T> newInstance(Factory<? extends T> factory) {
        return new LazyHolder<T>(factory);
    }
    
    protected final Factory<? extends T> factory;
    protected T instance;
    
    protected LazyHolder(Factory<? extends T> factory) {
        this.factory = factory;
        this.instance = null;
    }
    
    /**
     * Not thread-safe!
     */
    @Override
    public T get() {
        if (instance == null) {
            instance = factory.get();
        }
        return instance;
    }
}
