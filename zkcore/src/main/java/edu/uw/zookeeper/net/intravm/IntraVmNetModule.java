package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBufAllocator;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.protocol.Codec;

public class IntraVmNetModule implements NetClientModule, NetServerModule {

    public static IntraVmNetModule defaults() {
        return create(
                IntraVmCodecEndpointFactory.unpooled(),
                IntraVmCodecEndpointFactory.sameThreadExecutors(),
                IntraVmFactory.defaults());
    }

    public static IntraVmNetModule create(
            Factory<? extends ByteBufAllocator> allocators,
            Factory<? extends Executor> executors,
            IntraVmFactory factory) {
        return new IntraVmNetModule(
                allocators,
                executors,
                factory);
    }
    
    protected final Factory<? extends ByteBufAllocator> allocators;
    protected final IntraVmFactory factory;
    protected final Factory<? extends Executor> executors;
    
    public IntraVmNetModule(
            Factory<? extends ByteBufAllocator> allocators,
            Factory<? extends Executor> executors,
            IntraVmFactory factory) {
        this.factory = factory;
        this.allocators = allocators;
        this.executors = executors;
    }
    
    public Factory<? extends ByteBufAllocator> allocators() {
        return allocators;
    }
    
    public IntraVmFactory factory() {
        return factory;
    }
    
    public Factory<? extends Executor> executors() {
        return executors;
    }

    @Override
    public <I, T extends Codec<? super I, ? extends Optional<?>>, C extends Connection<?>> Factory<IntraVmClientConnectionFactory<C,I>> getClientConnectionFactory(
            final ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            final ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        final IntraVmCodecEndpointFactory<I,T> endpoints = getEndpointFactory(codecFactory);
        final ParameterizedFactory<IntraVmConnection<I>, C> connections = getConnectionFactory(connectionFactory);
        return new Factory<IntraVmClientConnectionFactory<C,I>>() {
            @Override
            public IntraVmClientConnectionFactory<C,I> get() {
                return factory.newClient(endpoints, connections);
            }
        };
    }
    
    @Override
    public <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<C>> getServerConnectionFactory(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        final IntraVmCodecEndpointFactory<I,T> endpoints = getEndpointFactory(codecFactory);
        final ParameterizedFactory<IntraVmConnection<I>, C> connections = getConnectionFactory(connectionFactory);
        return new ParameterizedFactory<SocketAddress, IntraVmServerConnectionFactory<C,I>>() {
            @Override
            public IntraVmServerConnectionFactory<C,I> get(SocketAddress value) {
                return factory.newServer(value, endpoints, connections);
            }
        };
    }

    public <I, T extends Codec<? super I, ? extends Optional<?>>> IntraVmCodecEndpointFactory<I,T> getEndpointFactory(
            final ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory) {
        return IntraVmCodecEndpointFactory.create(
                codecFactory, allocators, factory.addresses(), factory.publishers(), executors);
    }
    
    public <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> ParameterizedFactory<IntraVmConnection<I>, C> getConnectionFactory(
            final ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        return new ParameterizedFactory<IntraVmConnection<I>, C>() {
            @Override
            public C get(IntraVmConnection<I> value) {
                @SuppressWarnings("unchecked")
                IntraVmCodecEndpoint<I,T> endpoint = (IntraVmCodecEndpoint<I,T>) value.local();
                return connectionFactory.get(Pair.create(endpoint.getCodec(), (Connection<I>) value));
            }
        };
    }
}
