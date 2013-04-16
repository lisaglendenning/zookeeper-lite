package edu.uw.zookeeper.server;

import java.net.SocketAddress;

import edu.uw.zookeeper.ConnectionGroup;

public interface ServerConnectionGroup extends ConnectionGroup {

    SocketAddress localAddress();
}
