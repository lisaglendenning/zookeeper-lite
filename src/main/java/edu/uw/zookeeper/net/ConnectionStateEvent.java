package edu.uw.zookeeper.net;

import edu.uw.zookeeper.util.AutomatonTransition;

public class ConnectionStateEvent extends
        ConnectionEventValue<AutomatonTransition<Connection.State>> {

    public static ConnectionStateEvent create(Connection connection,
            AutomatonTransition<Connection.State> event) {
        return new ConnectionStateEvent(connection, event);
    }

    private ConnectionStateEvent(Connection connection, AutomatonTransition<Connection.State> event) {
        super(connection, event);
    }
}
