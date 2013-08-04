package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public interface ClientExecutor<I extends Operation.Request, V extends Operation.ProtocolResponse<Records.Response>> extends TaskExecutor<I, V>, Eventful {
    ListenableFuture<V> submit(I request, Promise<V> promise);
}
