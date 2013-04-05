package org.apache.zookeeper.event;

import org.apache.zookeeper.Connection;


public interface ConnectionEvent extends Event {
    
    Connection connection();
}
