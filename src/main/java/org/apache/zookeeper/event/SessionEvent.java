package org.apache.zookeeper.event;

import org.apache.zookeeper.Session;

public interface SessionEvent extends Event {
    Session session();
}
