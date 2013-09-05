package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public class IntraVmClientConnectionFactory<C extends Connection<?>, V> extends IntraVmConnectionFactory<C,V> implements ClientConnectionFactory<C> {

    public static <C extends Connection<?>, V> IntraVmClientConnectionFactory<C,V> newInstance(
            Function<SocketAddress, IntraVmServerConnectionFactory<?,?>> connector,
            Publisher publisher,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<V>, C> connectionFactory) {
        return new IntraVmClientConnectionFactory<C,V>(connector, publisher, endpointFactory, connectionFactory);
    }
    
    protected final Function<SocketAddress, IntraVmServerConnectionFactory<?,?>> connector;
    
    public IntraVmClientConnectionFactory(
            Function<SocketAddress, IntraVmServerConnectionFactory<?,?>> connector,
            Publisher publisher,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<V>, C> connectionFactory) {
        super(publisher, endpointFactory, connectionFactory);
        this.connector = connector;
    }
    
    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        IntraVmServerConnectionFactory<?,?> server = connector.apply(remoteAddress);
        if (server == null) {
            throw new IllegalArgumentException(String.valueOf(remoteAddress));
        }
        C connection = server.connect(endpointFactory.get(), connectionFactory);
        if (add(connection)) {
            return Futures.immediateFuture(connection);
        } else {
            return Futures.immediateFailedFuture(new IllegalStateException(Connection.State.CONNECTION_CLOSING.toString()));
        }
    }
}
