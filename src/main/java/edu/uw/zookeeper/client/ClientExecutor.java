package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection.RequestFuture;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ClientExecutor extends TaskExecutor<Operation.Request, Operation.SessionResult>, Eventful {
    RequestFuture submit(Operation.Request request, Promise<Operation.SessionResult> promise);
}
