package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.Event;
import edu.uw.zookeeper.Session;

@Event
public interface SessionEvent {
    Session session();
}
