package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class IntraVmConnectionFactory<T extends SocketAddress, I, C extends Connection<I>> extends AbstractConnectionFactory<I,C> {

    protected final ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory;
    protected final Set<C> connections;
    
    protected IntraVmConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        super(publisher);
        this.connections = Collections.synchronizedSet(Sets.<C>newHashSet());
        this.connectionFactory = connectionFactory;
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
