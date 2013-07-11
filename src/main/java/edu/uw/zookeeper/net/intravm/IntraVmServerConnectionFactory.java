package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class IntraVmServerConnectionFactory<T extends SocketAddress, I, C extends Connection<I>> extends IntraVmConnectionFactory<T,I,C> implements ServerConnectionFactory<I,C> {

    public static <T extends SocketAddress, I, C extends Connection<I>> IntraVmServerConnectionFactory<T,I,C> newInstance(
            T listenAddress,
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint<T>, IntraVmConnectionEndpoint<T>>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        return new IntraVmServerConnectionFactory<T,I,C>(listenAddress, publisher, endpointFactory, connectionFactory);
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
