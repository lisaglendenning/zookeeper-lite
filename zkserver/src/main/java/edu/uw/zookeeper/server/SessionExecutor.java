package edu.uw.zookeeper.server;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Session;

public interface SessionExecutor extends TaskExecutor<Message.ClientRequest<?>, Message.ServerResponse<?>>, PubSubSupport<Object> {
    Session session();
}
