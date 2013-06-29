package edu.uw.zookeeper.net.intravm;

import static com.google.common.base.Preconditions.*;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class IntraVmClientConnectionFactory<I, C extends Connection<I>> extends AbstractConnectionFactory<I,C> implements ClientConnectionFactory<I,C> {

    public static <I, C extends Connection<I>> IntraVmClientConnectionFactory<I,C> newInstance(
            Publisher publisher,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        return new IntraVmClientConnectionFactory<I,C>(publisher, connectionFactory);
    }
    
    protected final ParameterizedFactory<IntraVmConnection, C> connectionFactory;
    protected final Set<C> connections;
    
    public IntraVmClientConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<IntraVmConnection, C> connectionFactory) {
        super(publisher);
        this.connections = Collections.synchronizedSet(Sets.<C>newHashSet());
        this.connectionFactory = connectionFactory;
    }
    
    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        checkArgument(remoteAddress instanceof IntraVmSocketAddress);
        IntraVmSocketAddress address = (IntraVmSocketAddress) remoteAddress;
        checkArgument(address.get() instanceof IntraVmServerConnectionFactory);
        try {
            IntraVmConnection connection = ((IntraVmServerConnectionFactory<?,?>) address.get()).connect();
            C c = connectionFactory.get(connection);
            add(c);
            return Futures.immediateFuture(c);
        } catch (Exception e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
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
