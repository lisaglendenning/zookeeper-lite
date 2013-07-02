package edu.uw.zookeeper.client;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ClientExecutor extends TaskExecutor<Operation.ClientRequest, Operation.SessionResult>, Eventful {
}
