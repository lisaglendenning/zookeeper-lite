package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.ConnectionFactory;

public interface ServerConnectionFactory extends ConnectionFactory {
    SocketAddress listenAddress();
}
