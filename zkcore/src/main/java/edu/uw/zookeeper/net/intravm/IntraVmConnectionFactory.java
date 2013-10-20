package edu.uw.zookeeper.net.intravm;

import java.util.Collections;
import java.util.Set;

import net.engio.mbassy.PubSubSupport;

import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public abstract class IntraVmConnectionFactory<C extends Connection<?>, V> extends AbstractConnectionFactory<C> {

    protected final Factory<? extends IntraVmEndpoint<?>> endpointFactory;
    protected final ParameterizedFactory<? super IntraVmConnection<V>, C> connectionFactory;
    protected final Set<C> connections;
    
    protected IntraVmConnectionFactory(
            PubSubSupport<Object> publisher,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<? super IntraVmConnection<V>, C> connectionFactory) {
        super(publisher);
        this.connections = Collections.synchronizedSet(Sets.<C>newHashSet());
        this.connectionFactory = connectionFactory;
        this.endpointFactory = endpointFactory;
    }
    
    @Override
    protected Set<C> connections() {
        return connections;
    }
}
