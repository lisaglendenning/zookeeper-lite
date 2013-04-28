package edu.uw.zookeeper.event;

import io.netty.buffer.ByteBuf;
import edu.uw.zookeeper.net.Connection;

public class ConnectionBufferEvent extends ConnectionEventValue<ByteBuf> {

    public static ConnectionBufferEvent create(Connection connection,
            ByteBuf event) {
        return new ConnectionBufferEvent(connection, event);
    }

    private ConnectionBufferEvent(Connection connection, ByteBuf event) {
        super(connection, event);
    }
}
