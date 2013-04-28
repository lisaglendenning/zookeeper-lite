package edu.uw.zookeeper;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.TaskExecutor;

public interface SessionRequestExecutor extends TaskExecutor<Operation.SessionRequest, Operation.SessionReply> {
}
