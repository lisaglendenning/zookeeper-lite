package edu.uw.zookeeper.common;

import com.google.common.base.Objects;

public class PromiseTask<T,V> extends ForwardingPromise<V> {

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
    protected final Promise<V> delegate;

    public PromiseTask(T task, Promise<V> delegate) {
        super();
        this.task = task;
        this.delegate = delegate;
    }

    public T task() {
        return task;
    }

    @Override
    public String toString() {
        return toString(Objects.toStringHelper(this)).toString();
    }
    
    protected Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
        return toString.add("task", task).add("future", delegate);
    }

    @Override
    protected Promise<V> delegate() {
        return delegate;
    }
}
