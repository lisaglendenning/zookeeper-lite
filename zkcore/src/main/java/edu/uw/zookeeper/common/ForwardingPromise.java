package edu.uw.zookeeper.common;

import com.google.common.util.concurrent.ForwardingListenableFuture;

public abstract class ForwardingPromise<V> extends ForwardingListenableFuture<V> implements Promise<V> {

    @Override
    abstract protected Promise<V> delegate();
    
    @Override
    public boolean set(V value) {
        return delegate().set(value);
    }

    @Override
    public boolean setException(Throwable throwable) {
        return delegate().setException(throwable);
    }
    
    @Override
    public String toString() {
        return ToStringListenableFuture.toString(delegate());
    }
}
