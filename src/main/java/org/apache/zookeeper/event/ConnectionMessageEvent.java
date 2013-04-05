package org.apache.zookeeper.event;

import org.apache.zookeeper.Connection;

public class ConnectionMessageEvent<T> extends ConnectionEventValue<T> {

    public static <T> ConnectionMessageEvent<T> create(Connection connection, T event) {
        return new ConnectionMessageEvent<T>(connection, event);
    }
    
    protected ConnectionMessageEvent(Connection connection, T event) {
        super(connection, event);
    }
}
