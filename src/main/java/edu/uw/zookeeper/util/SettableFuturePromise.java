package edu.uw.zookeeper.util;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class SettableFuturePromise<V> extends ForwardingListenableFuture<V> implements Promise<V> {

    public static <V> SettableFuturePromise<V> create() {
        return wrap(SettableFuture.<V>create());
    }
    
    public static <V> SettableFuturePromise<V> wrap(SettableFuture<V> delegate) {
        return new SettableFuturePromise<V>(delegate);
    }
    
    protected final SettableFuture<V> delegate;
    
    protected SettableFuturePromise(SettableFuture<V> delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public boolean set(V value) {
        return delegate().set(value);
    }

    @Override
    public boolean setException(Throwable throwable) {
        return delegate().setException(throwable);
    }

    @Override
    protected SettableFuture<V> delegate() {
        return delegate;
    }
}
