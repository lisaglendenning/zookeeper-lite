package edu.uw.zookeeper.event;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.SessionConnection;

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
