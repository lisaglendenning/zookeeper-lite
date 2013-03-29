package org.apache.zookeeper;


public interface ConnectionEvent extends Event {
    
    Connection connection();
}
