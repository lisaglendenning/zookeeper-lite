package edu.uw.zookeeper.client;

import java.net.SocketAddress;


import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Connection;
import edu.uw.zookeeper.ConnectionGroup;

public interface ClientConnectionGroup extends ConnectionGroup {
    ListenableFuture<Connection> connect(SocketAddress remoteAddress);
}
