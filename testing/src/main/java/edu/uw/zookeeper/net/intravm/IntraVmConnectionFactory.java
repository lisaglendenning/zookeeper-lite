package edu.uw.zookeeper.net.intravm;

import java.util.Collections;
import java.util.Set;

import net.engio.mbassy.common.IConcurrentSet;

import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public abstract class IntraVmConnectionFactory<I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> extends AbstractConnectionFactory<C> {

    protected final Factory<? extends T> endpointFactory;
    protected final ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory;
    protected final Set<ConnectionListener> connections;
    
    protected IntraVmConnectionFactory(
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory,
            IConcurrentSet<ConnectionsListener<? super C>> listeners) {
        super(listeners);
        this.connections = Collections.synchronizedSet(Sets.<ConnectionListener>newHashSet());
        this.connectionFactory = connectionFactory;
        this.endpointFactory = endpointFactory;
    }
    
    @Override
    protected Set<ConnectionListener> connections() {
        return connections;
    }
}
