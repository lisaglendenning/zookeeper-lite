package org.apache.zookeeper.event;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.SessionConnection;

public class ConnectionSessionStateEvent extends
        ConnectionEventValue<SessionConnection.State> {

    public static ConnectionSessionStateEvent create(Connection connection,
            SessionConnection.State event) {
        return new ConnectionSessionStateEvent(connection, event);
    }

    protected ConnectionSessionStateEvent(Connection connection,
            SessionConnection.State event) {
        super(connection, event);
    }
}
