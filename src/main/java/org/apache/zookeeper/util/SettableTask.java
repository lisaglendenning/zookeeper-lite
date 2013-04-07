package org.apache.zookeeper.util;

import com.google.common.util.concurrent.SettableFuture;

public class SettableTask<T, V> extends Pair<T, SettableFuture<V>> {

    public static <T, V> SettableTask<T, V> create(T task) {
        return new SettableTask<T, V>(task);
    }

    protected SettableTask(T task) {
        super(task, SettableFuture.<V> create());
    }

    public void cancel() {
        future().cancel(true);
    }

    public T task() {
        return first();
    }

    public SettableFuture<V> future() {
        return second();
    }
}