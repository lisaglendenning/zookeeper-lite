package edu.uw.zookeeper.client;

import java.net.SocketAddress;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public class FixedClientConnectionFactory<C extends Connection<?>> extends Pair<SocketAddress, ClientConnectionFactory<C>> implements Factory<ListenableFuture<C>> {
    
    public static <C extends Connection<?>> FixedClientConnectionFactory<C> create(
            SocketAddress address,
            ClientConnectionFactory<C> connectionFactory) {
        return new FixedClientConnectionFactory<C>(address, connectionFactory);
    }
    
    protected FixedClientConnectionFactory(SocketAddress address,
            ClientConnectionFactory<C> connectionFactory) {
        super(address, connectionFactory);
    }
    
    @Override
    public ListenableFuture<C> get() {
        try {
            return second().connect(first());
        } catch (Throwable t) {
            return Futures.immediateFailedFuture(t);
        }
    }
}