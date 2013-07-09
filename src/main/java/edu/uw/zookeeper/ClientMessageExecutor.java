package edu.uw.zookeeper;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ClientMessageExecutor extends TaskExecutor<Message.Client, Message.Server> {
}
