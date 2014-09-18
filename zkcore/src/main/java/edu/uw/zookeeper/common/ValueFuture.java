package edu.uw.zookeeper.common;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListenableFuture;

public class ValueFuture<T,V, U extends ListenableFuture<V>> extends ToStringListenableFuture<V> {

    public static <T,V, U extends ListenableFuture<V>> ValueFuture<T,V,U> create(
            T value,
            U future) {
        return new ValueFuture<T,V,U>(value, future);
    }
    
    private final T value;
    private final U delegate;
    
    protected ValueFuture(
            T value,
            U delegate) {
        this.value = value;
        this.delegate = delegate;
    }
    
    public final T getValue() {
        return value;
    }
    
    @Override
    public U delegate() {
        return delegate;
    }
    
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(getValue()));
    }
}
