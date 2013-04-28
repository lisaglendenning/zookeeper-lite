package edu.uw.zookeeper.event;

import edu.uw.zookeeper.net.Connection;

public interface ConnectionEvent extends Event {
    Connection connection();
}
