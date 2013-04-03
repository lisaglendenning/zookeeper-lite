package org.apache.zookeeper;

import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.Eventful;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

public interface RequestExecutorService extends Eventful {
    
    public static interface Factory extends Provider<RequestExecutorService> {
        RequestExecutorService get();
        RequestExecutorService get(long sessionId);
    }
    
    ListenableFuture<Operation.Result> submit(Operation.Request request) throws InterruptedException;
}
