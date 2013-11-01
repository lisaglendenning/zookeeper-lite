package edu.uw.zookeeper.net;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.ConnectionFactory;

public interface ServerConnectionFactory<C extends Connection<?,?,?>> extends ConnectionFactory<C> {
    SocketAddress listenAddress();
}
