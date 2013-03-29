package org.apache.zookeeper;


public interface SessionEvent extends Event {
    Session session();
}
