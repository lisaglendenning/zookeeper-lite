package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;

public interface ClientExecutor<I extends Operation.Request, O extends Operation.ProtocolResponse<?>, T extends SessionListener> extends TaskExecutor<I,O>, Eventful<T> {
    ListenableFuture<O> submit(I request, Promise<O> promise);
}
