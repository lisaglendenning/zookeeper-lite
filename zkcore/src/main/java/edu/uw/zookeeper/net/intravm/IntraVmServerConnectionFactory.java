package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;

public class IntraVmServerConnectionFactory<C extends Connection<?>, V> extends IntraVmConnectionFactory<C,V> implements ServerConnectionFactory<C> {

    public static <C extends Connection<?>, V> IntraVmServerConnectionFactory<C,V> newInstance(
            SocketAddress listenAddress,
            PubSubSupport<Object> publisher,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<? super IntraVmConnection<V>, C> connectionFactory) {
        return new IntraVmServerConnectionFactory<C,V>(listenAddress, publisher, endpointFactory, connectionFactory);
    }
    
    protected final SocketAddress listenAddress;
    
    public IntraVmServerConnectionFactory(
            SocketAddress listenAddress,
            PubSubSupport<Object> publisher,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<? super IntraVmConnection<V>, C> connectionFactory) {
        super(publisher, endpointFactory, connectionFactory);
        this.listenAddress = listenAddress;
    }
    
    public <U,T> T connect(IntraVmEndpoint<?> remote, ParameterizedFactory<? super IntraVmConnection<U>, T> factory) {
        IntraVmEndpoint<?> local = endpointFactory.get();
        C localConnection = connectionFactory.get(IntraVmConnection.<V>create(local, remote));
        T remoteConnection = factory.get(IntraVmConnection.<U>create(remote, local));
        add(localConnection);
        return remoteConnection;
    }

    @Override
    public SocketAddress listenAddress() {
        return listenAddress;
    }
}
