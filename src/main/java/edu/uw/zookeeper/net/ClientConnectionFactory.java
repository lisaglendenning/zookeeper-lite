package edu.uw.zookeeper.net;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;


public interface ClientConnectionFactory extends ConnectionFactory {
    ListenableFuture<Connection> connect(SocketAddress remoteAddress);
}
