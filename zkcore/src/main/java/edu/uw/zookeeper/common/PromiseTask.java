package edu.uw.zookeeper.common;

import com.google.common.base.MoreObjects;

public class PromiseTask<T,V> extends ForwardingPromise.SimpleForwardingPromise<V> {

    public static <V> Promise<V> newPromise() {
        return SettableFuturePromise.<V>create();
    }
    
    public static <T,V> PromiseTask<T,V> of(T task) {
        Promise<V> promise = newPromise();
        return of(task, promise);
    }

    public static <T,V> PromiseTask<T,V> of(T task, Promise<V> promise) {
        return new PromiseTask<T,V>(task, promise);
    }
    
    protected final T task;

    public PromiseTask(T task, Promise<V> delegate) {
        super(delegate);
        this.task = task;
    }

    public T task() {
        return task;
    }
    
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper helper) {
        return super.toStringHelper(helper.addValue(task()));
    }
}
