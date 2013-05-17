package edu.uw.zookeeper.util;

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

    protected PromiseTask(T task, Promise<V> delegate) {
        super();
        this.task = task;
        this.delegate = delegate;
    }

    public T task() {
        return task;
    }

    @Override
    public String toString() {
        String futureString = Objects.toStringHelper(delegate())
                .add("isDone", delegate().isDone())
                .toString();
        return Objects.toStringHelper(this)
                .add("task", task())
                .add("future", futureString)
                .toString();
    }

    @Override
    protected Promise<V> delegate() {
        return delegate;
    }
}
