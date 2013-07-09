package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ClientExecutor<I extends Operation.Request, T extends Operation.SessionRequest, V extends Operation.SessionResponse> extends TaskExecutor<I, Pair<T,V>>, Eventful {
}
