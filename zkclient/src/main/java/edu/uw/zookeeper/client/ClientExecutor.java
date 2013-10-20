package edu.uw.zookeeper.client;

import net.engio.mbassy.PubSubSupport;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Operation;

public interface ClientExecutor<I extends Operation.Request, V extends Operation.ProtocolResponse<?>> extends TaskExecutor<I, V>, PubSubSupport<Object> {
    ListenableFuture<V> submit(I request, Promise<V> promise);
}
