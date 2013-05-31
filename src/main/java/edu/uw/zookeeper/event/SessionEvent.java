package edu.uw.zookeeper.event;

import edu.uw.zookeeper.util.Event;
import edu.uw.zookeeper.Session;

@Event
public interface SessionEvent {
    Session session();
}
