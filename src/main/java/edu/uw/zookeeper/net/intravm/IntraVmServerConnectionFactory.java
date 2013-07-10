package edu.uw.zookeeper.net.intravm;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class IntraVmServerConnectionFactory<I, C extends Connection<I>> extends IntraVmConnectionFactory<I,C> implements ServerConnectionFactory<I,C> {

    public static <I, C extends Connection<I>> IntraVmServerConnectionFactory<I,C> newInstance(
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        return new IntraVmServerConnectionFactory<I,C>(publisher, endpointFactory, connectionFactory);
    }
    
    protected final Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory;
    protected final IntraVmSocketAddress listenAddress;
    
    public IntraVmServerConnectionFactory(
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        super(publisher, connectionFactory);
        this.listenAddress = IntraVmSocketAddress.of(this);
        this.endpointFactory = endpointFactory;
    }
    
    public IntraVmConnection connect() {
        Pair<IntraVmConnection, IntraVmConnection> connections = IntraVmConnection.createPair(endpointFactory.get());
        C local = connectionFactory.get(connections.first());
        add(local);
        return connections.second();
    }

    @Override
    public IntraVmSocketAddress listenAddress() {
        return listenAddress;
    }
}
