package edu.uw.zookeeper;


import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Provider;

import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Eventful;

public interface RequestExecutorService extends Eventful {

    public static interface Factory extends Provider<RequestExecutorService> {
        RequestExecutorService get();

        RequestExecutorService get(long sessionId);
    }

    ListenableFuture<Operation.Result> submit(Operation.Request request);
}
