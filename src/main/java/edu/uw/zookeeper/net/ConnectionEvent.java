package edu.uw.zookeeper.net;

import edu.uw.zookeeper.Event;

@Event
public interface ConnectionEvent {
    Connection connection();
}
