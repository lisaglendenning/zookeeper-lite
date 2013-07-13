package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ClientExecutor<I extends Operation.Request, T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> extends TaskExecutor<I, Pair<T,V>>, Eventful {
    ListenableFuture<Pair<T,V>> submit(I request, Promise<Pair<T,V>> promise);
}
