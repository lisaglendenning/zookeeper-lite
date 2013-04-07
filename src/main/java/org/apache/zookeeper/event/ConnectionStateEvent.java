package org.apache.zookeeper.event;

import org.apache.zookeeper.Connection;

public class ConnectionStateEvent extends
        ConnectionEventValue<Connection.State> {

    public static ConnectionStateEvent create(Connection connection,
            Connection.State event) {
        return new ConnectionStateEvent(connection, event);
    }

    protected ConnectionStateEvent(Connection connection, Connection.State event) {
        super(connection, event);
    }
}
