package edu.uw.zookeeper.util;

import com.google.common.util.concurrent.ListenableFuture;


public interface TaskExecutor<I,O> {
    ListenableFuture<O> submit(I request);
}
