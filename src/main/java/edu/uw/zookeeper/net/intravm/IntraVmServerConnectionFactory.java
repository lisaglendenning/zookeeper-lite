package edu.uw.zookeeper.net.intravm;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class IntraVmServerConnectionFactory<I, C extends Connection<I>> extends AbstractConnectionFactory<I,C> implements ServerConnectionFactory<I,C> {

    public static <I, C extends Connection<I>> IntraVmServerConnectionFactory<I,C> newInstance(
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        return new IntraVmServerConnectionFactory<I,C>(publisher, endpointFactory, connectionFactory);
    }
    
    protected final Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory;
    protected final ParameterizedFactory<IntraVmConnection, C> connectionFactory;
    protected final IntraVmSocketAddress listenAddress;
    protected final Set<C> connections;
    
    public IntraVmServerConnectionFactory(
            Publisher publisher,
            Factory<Pair<IntraVmConnectionEndpoint, IntraVmConnectionEndpoint>> endpointFactory,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        super(publisher);
        this.listenAddress = IntraVmSocketAddress.of(this);
        this.connections = Collections.synchronizedSet(Sets.<C>newHashSet());
        this.endpointFactory = endpointFactory;
        this.connectionFactory = connectionFactory;
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

    @Override
    public Iterator<C> iterator() {
        return connections.iterator();
    }

    @Override
    protected boolean add(C connection) {
        connections.add(connection);
        return super.add(connection);
    }
    
    @Override
    protected boolean remove(C connection) {
        return connections.remove(connection);
    }
}
