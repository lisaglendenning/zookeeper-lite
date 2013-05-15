package edu.uw.zookeeper.util;

import com.google.common.util.concurrent.ListenableFuture;

public interface Promise<V> extends ListenableFuture<V> {
    boolean set(V value);
    boolean setException(Throwable throwable);
}
