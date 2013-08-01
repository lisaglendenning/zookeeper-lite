package edu.uw.zookeeper.net;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;


public interface ClientConnectionFactory<C extends Connection<?>> extends ConnectionFactory<C> {
    ListenableFuture<C> connect(SocketAddress remoteAddress);
}
