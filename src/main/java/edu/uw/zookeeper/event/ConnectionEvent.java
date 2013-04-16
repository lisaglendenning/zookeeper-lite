package edu.uw.zookeeper.event;

import edu.uw.zookeeper.Connection;

public interface ConnectionEvent extends Event {

    Connection connection();
}
