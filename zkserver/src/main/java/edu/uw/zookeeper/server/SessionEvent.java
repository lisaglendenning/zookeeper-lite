package edu.uw.zookeeper.server;

import edu.uw.zookeeper.common.Event;
import edu.uw.zookeeper.protocol.Session;

@Event
public interface SessionEvent {
    Session session();
}
