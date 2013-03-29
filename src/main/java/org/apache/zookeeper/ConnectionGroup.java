package org.apache.zookeeper;

import java.net.SocketAddress;

import org.apache.zookeeper.util.Eventful;

public interface ConnectionGroup extends Iterable<Connection>, Eventful {
    Connection get(SocketAddress remoteAddress);
}
