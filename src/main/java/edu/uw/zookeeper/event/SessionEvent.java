package edu.uw.zookeeper.event;

import edu.uw.zookeeper.Event;
import edu.uw.zookeeper.Session;

@Event
public interface SessionEvent {
    Session session();
}
