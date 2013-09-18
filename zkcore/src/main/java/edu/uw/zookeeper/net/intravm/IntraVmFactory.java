package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.MapMaker;

import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;

public class IntraVmFactory {

    public static IntraVmFactory defaults() {
        return newInstance(
                IntraVmEndpointFactory.loopbackAddresses(1),
                EventBusPublisher.factory());
    }
    
    public static IntraVmFactory newInstance(
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Publisher> publishers) {
        return new IntraVmFactory(addresses, publishers);
    }
    
    protected final ConcurrentMap<SocketAddress, IntraVmServerConnectionFactory<?,?>> servers;
    protected final Function<SocketAddress, IntraVmServerConnectionFactory<?,?>> connector;
    protected final Supplier<? extends Publisher> publishers;
    protected final Supplier<? extends SocketAddress> addresses;
    
    public IntraVmFactory(
            Supplier<? extends SocketAddress> addresses,
            Supplier<? extends Publisher> publishers) {
        this.addresses = addresses;
        this.publishers = publishers;
        this.servers = new MapMaker().makeMap();
        this.connector = new Function<SocketAddress, IntraVmServerConnectionFactory<?,?>>() {
            @Override
            public @Nullable IntraVmServerConnectionFactory<?,?> apply(SocketAddress input) {
                return servers.get(input);
            }
        };
    }

    public Supplier<? extends SocketAddress> addresses() {
        return addresses;
    }
    
    public Supplier<? extends Publisher> publishers() {
        return publishers;
    }

    public <C extends Connection<?>, V> IntraVmServerConnectionFactory<C,V> newServer(
            SocketAddress listenAddress,
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<? super IntraVmConnection<V>, C> connectionFactory) {
        Publisher publisher = LoggingPublisher.create(
                LogManager.getLogger(IntraVmServerConnectionFactory.class),
                publishers.get(),
                listenAddress);
        IntraVmServerConnectionFactory<C,V> server = IntraVmServerConnectionFactory.newInstance(
                listenAddress, publisher, endpointFactory, connectionFactory);
        IntraVmServerConnectionFactory<?,?> prev = servers.putIfAbsent(listenAddress, server);
        if (prev != null) {
            throw new IllegalArgumentException(String.valueOf(listenAddress));
        }
        return server;
    }
    
    public <C extends Connection<?>, V> IntraVmClientConnectionFactory<C,V> newClient(
            Factory<? extends IntraVmEndpoint<?>> endpointFactory,
            ParameterizedFactory<IntraVmConnection<V>, C> connectionFactory) {
        Publisher publisher = LoggingPublisher.create(
                LogManager.getLogger(IntraVmClientConnectionFactory.class),
                publishers.get());
        IntraVmClientConnectionFactory<C,V> client = IntraVmClientConnectionFactory.newInstance(
                connector, publisher, endpointFactory, connectionFactory);
        return client;
    }
}
