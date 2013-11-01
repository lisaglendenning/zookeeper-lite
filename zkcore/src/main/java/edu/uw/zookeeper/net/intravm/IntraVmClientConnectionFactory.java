package edu.uw.zookeeper.net.intravm;

import static com.google.common.base.Preconditions.checkArgument;

import java.net.SocketAddress;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public class IntraVmClientConnectionFactory<I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> extends IntraVmConnectionFactory<I,O,U,V,T,C> implements ClientConnectionFactory<C> {

    public static <I,O,U,V, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> IntraVmClientConnectionFactory<I,O,U,V,T,C> weakListeners(
            Function<SocketAddress, ? extends IntraVmServerConnectionFactory<?,?,? extends V,? super U,?,?>> connector,
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory) {
        return new IntraVmClientConnectionFactory<I,O,U,V,T,C>(connector, endpointFactory, connectionFactory, new WeakConcurrentSet<ConnectionsListener<? super C>>());
    }
    
    protected final Function<SocketAddress, ? extends IntraVmServerConnectionFactory<?,?,? extends V,? super U,?,?>> connector;
    
    public IntraVmClientConnectionFactory(
            Function<SocketAddress, ? extends IntraVmServerConnectionFactory<?,?,? extends V,? super U,?,?>> connector,
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory,
            IConcurrentSet<ConnectionsListener<? super C>> listeners) {
        super(endpointFactory, connectionFactory, listeners);
        this.connector = connector;
    }
    
    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        logger.debug("Connecting => {}", remoteAddress);
        IntraVmServerConnectionFactory<?,?,? extends V,? super U,?,?> server = connector.apply(remoteAddress);
        checkArgument(server != null, remoteAddress);
        T local = endpointFactory.get();
        AbstractIntraVmEndpoint<?,?,?,? super U> remote = server.connect(local);
        C connection = connectionFactory.get(Pair.create(local, remote));
        if (add(connection)) {
            return Futures.immediateFuture(connection);
        } else {
            return Futures.immediateFailedFuture(new IllegalStateException(String.valueOf(state())));
        }
    }
}
