package edu.uw.zookeeper.event;

import edu.uw.zookeeper.Event;
import edu.uw.zookeeper.Session;

public interface SessionEvent extends Event {
    Session session();
}
