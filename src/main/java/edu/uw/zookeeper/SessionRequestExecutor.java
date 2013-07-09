package edu.uw.zookeeper;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.TaskExecutor;

public interface SessionRequestExecutor extends TaskExecutor<Message.ClientRequest, Message.ServerResponse>, Eventful {
}
