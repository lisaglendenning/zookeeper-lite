package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public class IntraVmClientConnectionFactory<T extends SocketAddress, C extends Connection<?>> extends IntraVmConnectionFactory<T,C> implements ClientConnectionFactory<C> {

    public static <T extends SocketAddress, C extends Connection<?>> IntraVmClientConnectionFactory<T, C> newInstance(
            Function<SocketAddress, IntraVmConnection<T>> connector,
            Publisher publisher,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        return new IntraVmClientConnectionFactory<T, C>(connector, publisher, connectionFactory);
    }
    
    protected final Function<SocketAddress, IntraVmConnection<T>> connector;
    
    public IntraVmClientConnectionFactory(
            Function<SocketAddress, IntraVmConnection<T>> connector,
            Publisher publisher,
            ParameterizedFactory<IntraVmConnection<T>, C> connectionFactory) {
        super(publisher, connectionFactory);
        this.connector = connector;
    }
    
    @Override
    public ListenableFuture<C> connect(SocketAddress remoteAddress) {
        try {
            IntraVmConnection<T> connection = connector.apply(remoteAddress);
            C c = connectionFactory.get(connection);
            add(c);
            return Futures.immediateFuture(c);
        } catch (Exception e) {
            return Futures.immediateFailedCheckedFuture(e);
        }
    }
}
