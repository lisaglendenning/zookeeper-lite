package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;

public class IntraVmFactory<U,V> {

    public static <U,V> IntraVmFactory<U,V> defaults() {
        return newInstance(
                IntraVmEndpointFactory.loopbackAddresses(1));
    }
    
    public static <U,V> IntraVmFactory<U,V> newInstance(
            Supplier<? extends SocketAddress> addresses) {
        return new IntraVmFactory<U,V>(addresses);
    }
    
    protected final ConcurrentMap<SocketAddress, IntraVmServerConnectionFactory<?,?,V,U,?,?>> servers;
    protected final Supplier<? extends SocketAddress> addresses;
    
    public IntraVmFactory(
            Supplier<? extends SocketAddress> addresses) {
        this.addresses = addresses;
        this.servers = new MapMaker().makeMap();
    }

    public Supplier<? extends SocketAddress> addresses() {
        return addresses;
    }
    
    public <I,O, T extends AbstractIntraVmEndpoint<I,O,V,U>, C extends Connection<?,?,?>> IntraVmServerConnectionFactory<I,O,V,U,T,C> newServer(
            SocketAddress listenAddress,
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super V>>, ? extends C> connectionFactory) {
        IntraVmServerConnectionFactory<I,O,V,U,T,C> server = IntraVmServerConnectionFactory.weakListeners(
                listenAddress, endpointFactory, connectionFactory);
        IntraVmServerConnectionFactory<?,?,V,U,?,?> prev = servers.putIfAbsent(listenAddress, server);
        if (prev != null) {
            throw new IllegalArgumentException(String.valueOf(listenAddress));
        }
        return server;
    }
    
    public <I,O, T extends AbstractIntraVmEndpoint<I,O,U,V>, C extends Connection<?,?,?>> IntraVmClientConnectionFactory<I,O,U,V,T,C> newClient(
            Factory<? extends T> endpointFactory,
            ParameterizedFactory<Pair<? extends T, ? extends AbstractIntraVmEndpoint<?,?,?,? super U>>, ? extends C> connectionFactory) {
        IntraVmClientConnectionFactory<I,O,U,V,T,C> client = IntraVmClientConnectionFactory.<I,O,U,V,T,C>weakListeners(
                Functions.forMap(servers), endpointFactory, connectionFactory);
        return client;
    }
}
