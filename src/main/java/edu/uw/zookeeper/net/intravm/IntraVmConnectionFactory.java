package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public abstract class IntraVmConnectionFactory<T extends SocketAddress, C extends Connection<?>> extends AbstractConnectionFactory<C> {

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
    protected Set<C> connections() {
        return connections;
    }
}
