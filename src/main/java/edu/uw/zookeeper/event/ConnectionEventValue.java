package edu.uw.zookeeper.event;


import io.netty.buffer.ByteBuf;

import com.google.common.base.Objects;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.AutomatonTransition;

public abstract class ConnectionEventValue<T> extends AbstractPair<Connection, T> implements
        ConnectionEvent {

    @SuppressWarnings("unchecked")
    public static ConnectionEventValue<?> create(Connection connection,
            Object event) {
        ConnectionEventValue<?> connectionEvent = null;
        if (event instanceof AutomatonTransition) {
            connectionEvent = ConnectionStateEvent
                    .create(connection, (AutomatonTransition<Connection.State>) event);
        } else if (event instanceof ByteBuf) {
            connectionEvent = ConnectionBufferEvent.create(connection, (ByteBuf) event);
        } else {
            throw new IllegalArgumentException();
        }
        return connectionEvent;
    }

    protected ConnectionEventValue(Connection connection, T event) {
        super(connection, event);
    }

    @Override
    public Connection connection() {
        return first;
    }

    public T event() {
        return second;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("connection", connection())
                .add("event", event()).toString();
    }
}
