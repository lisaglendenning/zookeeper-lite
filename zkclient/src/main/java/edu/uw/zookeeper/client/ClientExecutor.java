package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Operation;

public interface ClientExecutor<I extends Operation.Request, V extends Operation.ProtocolResponse<?>> extends TaskExecutor<I, V>, Publisher {
    ListenableFuture<V> submit(I request, Promise<V> promise);
}