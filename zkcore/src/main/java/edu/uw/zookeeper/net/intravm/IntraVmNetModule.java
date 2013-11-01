package edu.uw.zookeeper.net.intravm;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import com.google.common.base.Supplier;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.net.ServerConnectionFactory;

public class IntraVmNetModule implements NetClientModule, NetServerModule {

    public static IntraVmNetModule defaults() {
        return create(
                IntraVmCodecEndpointFactory.unpooled(),
                IntraVmCodecEndpointFactory.sameThreadExecutors(),
                IntraVmFactory.<ByteBuf,ByteBuf>defaults());
    }

    public static IntraVmNetModule create(
            Factory<? extends ByteBufAllocator> allocators,
            Factory<? extends Executor> executors,
            IntraVmFactory<ByteBuf,ByteBuf> factory) {
        return new IntraVmNetModule(
                allocators,
                executors,
                factory);
    }
    
    protected final Supplier<? extends ByteBufAllocator> allocators;
    protected final IntraVmFactory<ByteBuf,ByteBuf> factory;
    protected final Supplier<? extends Executor> executors;
    
    public IntraVmNetModule(
            Supplier<? extends ByteBufAllocator> allocators,
            Factory<? extends Executor> executors,
            IntraVmFactory<ByteBuf,ByteBuf> factory) {
        this.factory = factory;
        this.allocators = allocators;
        this.executors = executors;
    }
    
    public Supplier<? extends ByteBufAllocator> allocators() {
        return allocators;
    }
    
    public IntraVmFactory<ByteBuf,ByteBuf> factory() {
        return factory;
    }
    
    public Supplier<? extends Executor> executors() {
        return executors;
    }

    @Override
    public <I, O, T extends Codec<I, O, ? extends I, ? extends O>, C extends Connection<?,?,?>> Factory<? extends ClientConnectionFactory<C>> getClientConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        final IntraVmCodecEndpointFactory<I,O,T> endpoints = getEndpointFactory(codecFactory);
        final ParameterizedFactory<Pair<? extends IntraVmCodecEndpoint<I,O,T>, ? extends AbstractIntraVmEndpoint<?,?,?,? super ByteBuf>>, ? extends C> connections = getConnectionFactory(connectionFactory);
        return new Factory<ClientConnectionFactory<C>>() {
            @Override
            public ClientConnectionFactory<C> get() {
                return factory.newClient(endpoints, connections);
            }
        };
    }
    
    @Override
    public <I, O, T extends Codec<I, O, ? extends I, ? extends O>, C extends Connection<?,?,?>> ParameterizedFactory<SocketAddress, ? extends ServerConnectionFactory<C>> getServerConnectionFactory(
            Factory<? extends T> codecFactory,
            ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        final IntraVmCodecEndpointFactory<I,O,T> endpoints = getEndpointFactory(codecFactory);
        final ParameterizedFactory<Pair<? extends IntraVmCodecEndpoint<I,O,T>, ? extends AbstractIntraVmEndpoint<?,?,?,? super ByteBuf>>, ? extends C> connections = getConnectionFactory(connectionFactory);
        return new ParameterizedFactory<SocketAddress, ServerConnectionFactory<C>>() {
            @Override
            public ServerConnectionFactory<C> get(SocketAddress value) {
                return factory.newServer(value, endpoints, connections);
            }
        };
    }

    public <I, O, T extends Codec<I, O, ? extends I, ? extends O>> IntraVmCodecEndpointFactory<I,O,T> getEndpointFactory(
            Supplier<? extends T> codecFactory) {
        return IntraVmCodecEndpointFactory.create(
                codecFactory, allocators, factory.addresses(), executors);
    }
    
    public <I, O, T extends Codec<I, O, ? extends I, ? extends O>, C extends Connection<?,?,?>> ParameterizedFactory<Pair<? extends IntraVmCodecEndpoint<I,O,T>, ? extends AbstractIntraVmEndpoint<?,?,?,? super ByteBuf>>, ? extends C> getConnectionFactory(
            final ParameterizedFactory<CodecConnection<I,O,T,?>, C> connectionFactory) {
        return new ParameterizedFactory<Pair<? extends IntraVmCodecEndpoint<I,O,T>, ? extends AbstractIntraVmEndpoint<?,?,?,? super ByteBuf>>, C>() {
            @Override
            public C get(Pair<? extends IntraVmCodecEndpoint<I,O,T>, ? extends AbstractIntraVmEndpoint<?,?,?,? super ByteBuf>> value) {
                IntraVmCodecConnection<I,O,T> endpoint = IntraVmCodecConnection.newInstance(value.first().getCodec(), value.first(), value.second());
                return connectionFactory.get(endpoint);
            }
        };
    }
}
