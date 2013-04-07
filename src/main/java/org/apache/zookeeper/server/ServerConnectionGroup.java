package org.apache.zookeeper.server;

import java.net.SocketAddress;

import org.apache.zookeeper.ConnectionGroup;

public interface ServerConnectionGroup extends ConnectionGroup {

    SocketAddress localAddress();
}
