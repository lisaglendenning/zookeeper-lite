package org.apache.zookeeper.client;

import java.net.SocketAddress;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionGroup;

import com.google.common.util.concurrent.ListenableFuture;


public interface ClientConnectionGroup extends ConnectionGroup {
    ListenableFuture<Connection> connect(SocketAddress remoteAddress);
}
