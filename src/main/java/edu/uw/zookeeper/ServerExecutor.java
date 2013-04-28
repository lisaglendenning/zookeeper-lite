package edu.uw.zookeeper;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.TaskExecutor;

public interface ServerExecutor extends TaskExecutor<Message.ClientMessage, Message.ServerMessage> {
}
