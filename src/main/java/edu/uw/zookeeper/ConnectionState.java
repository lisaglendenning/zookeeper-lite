package edu.uw.zookeeper;


import com.google.inject.Inject;

import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.EventfulAutomataState;

public class ConnectionState extends EventfulAutomataState<Connection.State> {

    public static ConnectionState create(Eventful eventful) {
        return new ConnectionState(eventful);
    }

    public static ConnectionState create(Eventful eventful,
            Connection.State state) {
        return new ConnectionState(eventful, state);
    }

    @Inject
    protected ConnectionState(Eventful eventful) {
        this(eventful, Connection.State.CONNECTION_OPENING);
    }

    protected ConnectionState(Eventful eventful, Connection.State state) {
        super(eventful, state);
    }
}
