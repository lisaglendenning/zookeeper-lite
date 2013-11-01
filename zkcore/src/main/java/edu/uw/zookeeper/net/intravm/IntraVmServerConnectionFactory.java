package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;

public class IntraVmServerConnectionFactory<I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> extends IntraVmConnectionFactory<I,O,U,V,T,C> implements ServerConnectionFactory<C> {

    public static <I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> IntraVmServerConnectionFactory<I,O,U,V,T,C> weakListeners(
            SocketAddress listenAddress,
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory) {
        return new IntraVmServerConnectionFactory<I,O,U,V,T,C>(listenAddress, endpointFactory, connectionFactory, new WeakConcurrentSet<ConnectionsListener<? super C>>());
    }
    
    protected final SocketAddress listenAddress;
    
    public IntraVmServerConnectionFactory(
            SocketAddress listenAddress,
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory,
            IConcurrentSet<ConnectionsListener<? super C>> listeners) {
        super(endpointFactory, connectionFactory, listeners);
        this.listenAddress = listenAddress;
    }
    
    public T connect(AbstractIntraVmEndpoint<?,?,?,? super U> remote) {
        T local = endpointFactory.get();
        C localConnection = connectionFactory.get(Pair.create(local, remote));
        add(localConnection);
        return local;
    }

    @Override
    public SocketAddress listenAddress() {
        return listenAddress;
    }
}
