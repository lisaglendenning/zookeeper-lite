package edu.uw.zookeeper.net;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;


public interface ClientConnectionFactory<I, C extends Connection<I>> extends ConnectionFactory<I,C> {
    ListenableFuture<C> connect(SocketAddress remoteAddress);
}
