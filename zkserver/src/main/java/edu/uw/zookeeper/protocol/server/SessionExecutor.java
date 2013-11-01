package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Stateful;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;

public interface SessionExecutor extends TaskExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>>, Stateful<ProtocolState>, Eventful<SessionListener> {

    Session session();
}
