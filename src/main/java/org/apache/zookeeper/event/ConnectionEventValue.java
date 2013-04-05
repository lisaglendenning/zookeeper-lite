package org.apache.zookeeper.event;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.Connection.State;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.util.Pair;

import com.google.common.base.Objects;


public class ConnectionEventValue<T> extends Pair<Connection, T> implements ConnectionEvent {
    
    @SuppressWarnings("unchecked")
    public static <T> ConnectionEventValue<T> create(Connection connection, T event) {
    	ConnectionEventValue<T> connectionEvent = null;
        if (event instanceof Connection.State) {
        	connectionEvent = (ConnectionEventValue<T>) ConnectionStateEvent.create(connection, (Connection.State)event);
        } else if (event instanceof SessionConnection.State) {
        	connectionEvent = (ConnectionEventValue<T>) ConnectionSessionStateEvent.create(connection, (SessionConnection.State)event);
        } else {
        	connectionEvent = ConnectionMessageEvent.create(connection, event);
        }
        return connectionEvent;
    }
    
    protected ConnectionEventValue(Connection connection, T event) {
        super(connection, event);
    }
    
    @Override
    public Connection connection() {
        return first();
    }

    public T event() {
        return second();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("connection", connection())
                .add("event", event())
                .toString();
    }
}
