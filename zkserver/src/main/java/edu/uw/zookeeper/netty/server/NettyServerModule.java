package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.bus.SyncBusConfiguration;
import net.engio.mbassy.bus.SyncMessageBus;

import com.google.common.base.Optional;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.protocol.Codec;

public class NettyServerModule implements NetServerModule {

    public static NettyServerModule newInstance(
            RuntimeModule runtime) {
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.getThreadFactory(), runtime.getServiceMonitor()));
        return newInstance(syncMessageBus(), bootstrapFactory);
    }

    public static Factory<SyncMessageBus<Object>> syncMessageBus() {
        return new Factory<SyncMessageBus<Object>>() {
            @SuppressWarnings("rawtypes")
            @Override
            public SyncMessageBus<Object> get() {
                return new SyncMessageBus<Object>(new SyncBusConfiguration());
            }
        };
    }

    public static NettyServerModule newInstance(
            Factory<? extends PubSubSupport<Object>> publisherFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        return new NettyServerModule(publisherFactory, bootstrapFactory);
    }
    
    protected final Factory<? extends PubSubSupport<Object>> publisherFactory;
    protected final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory;
    
    public NettyServerModule(
            Factory<? extends PubSubSupport<Object>> publisherFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        this.publisherFactory = publisherFactory;
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<C>> getServerConnectionFactory(
            ParameterizedFactory<PubSubSupport<Object>, ? extends Pair<Class<I>, ? extends T>> codecFactory,
            ParameterizedFactory<Pair<? extends Pair<Class<I>, ? extends T>, Connection<I>>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(publisherFactory, codecFactory, connectionFactory);
        return ChannelServerConnectionFactory.parameterizedFactory(publisherFactory, factory, bootstrapFactory);
    }
}
