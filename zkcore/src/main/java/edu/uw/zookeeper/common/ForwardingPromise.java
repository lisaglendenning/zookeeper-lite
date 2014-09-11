package edu.uw.zookeeper.common;

import com.google.common.base.MoreObjects;
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
        return toStringHelper().toString();
    }
    
    protected MoreObjects.ToStringHelper toStringHelper() {
        return toStringHelper(MoreObjects.toStringHelper(this));
    }
    
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return helper.addValue(ToStringListenableFuture.toString3rdParty(delegate()));
    }
    
    public static class SimpleForwardingPromise<V> extends ForwardingPromise<V> {
        
        private final Promise<V> delegate;
        
        protected SimpleForwardingPromise(Promise<V> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        protected final Promise<V> delegate() {
            return delegate;
        }
    }
}
