package edu.uw.zookeeper.net;

import edu.uw.zookeeper.Event;

public interface ConnectionEvent extends Event {
    Connection connection();
}
