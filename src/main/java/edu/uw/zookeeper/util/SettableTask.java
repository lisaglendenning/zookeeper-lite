package edu.uw.zookeeper.util;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.SettableFuture;

public class SettableTask<T, V> extends AbstractPair<T, SettableFuture<V>> {

    public static <T, V> SettableTask<T, V> create(T task) {
        return new SettableTask<T, V>(task);
    }

    protected SettableTask(T task) {
        super(task, SettableFuture.<V> create());
    }

    public boolean cancel() {
        return future().cancel(true);
    }

    public T task() {
        return first;
    }

    public SettableFuture<V> future() {
        return second;
    }

    @Override
    public String toString() {
        String futureString = Objects.toStringHelper(future())
                .add("isDone", future().isDone())
                .toString();
        return Objects.toStringHelper(this)
                .add("task", task())
                .add("future", futureString)
                .toString();
    }
}