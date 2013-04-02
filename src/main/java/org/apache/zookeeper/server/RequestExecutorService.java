package org.apache.zookeeper.server;

import org.apache.zookeeper.protocol.Operation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

public interface RequestExecutorService {
    
    public static interface Factory extends Provider<RequestExecutorService> {
        RequestExecutorService get();
        RequestExecutorService get(long sessionId);
    }
    
    ListenableFuture<Operation.Result> submit(Operation.Request request) throws InterruptedException;
}
