package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.ConnectionFactory;

public interface ServerConnectionFactory<I, C extends Connection<I>> extends ConnectionFactory<I,C> {
    SocketAddress listenAddress();
}
