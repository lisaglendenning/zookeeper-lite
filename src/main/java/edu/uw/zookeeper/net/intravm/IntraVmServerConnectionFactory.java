package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;

public class IntraVmServerConnectionFactory<T extends SocketAddress, C extends Connection<?>> extends IntraVmConnectionFactory<T,C> implements ServerConnectionFactory<C> {

    public static <T extends SocketAddress, C extends Connection<?>> IntraVmServerConnectionFactory<T,C> newInstance(
            T listenAddress,
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        return new IntraVmServerConnectionFactory<T,C>(listenAddress, publisher, endpointFactory, connectionFactory);
    }
    
    protected final Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> endpointFactory;
    protected final T listenAddress;
    
    public IntraVmServerConnectionFactory(
            T listenAddress,
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        super(publisher, connectionFactory);
        this.listenAddress = listenAddress;
        this.endpointFactory = endpointFactory;
    }
    
    public IntraVmConnection<T> connect() {
        Pair<IntraVmConnection<T>, IntraVmConnection<T>> connections = IntraVmConnection.createPair(endpointFactory.get());
        C local = connectionFactory.get(connections.first());
        add(local);
        return connections.second();
    }

    @Override
    public T listenAddress() {
        return listenAddress;
    }
}
