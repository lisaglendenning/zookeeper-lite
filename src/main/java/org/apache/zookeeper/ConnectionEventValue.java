package org.apache.zookeeper;

import org.apache.zookeeper.util.Pair;

import com.google.common.base.Objects;


public class ConnectionEventValue<T> extends Pair<Connection, T> implements ConnectionEvent {
    
    @SuppressWarnings("unchecked")
    public static <T> ConnectionEventValue<T> create(Connection connection, T event) {
        if (event instanceof Connection.State) {
            return (ConnectionEventValue<T>) ConnectionStateEvent.create(connection, (Connection.State)event);
        } else {
            return new ConnectionEventValue<T>(connection, event);
        }
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
