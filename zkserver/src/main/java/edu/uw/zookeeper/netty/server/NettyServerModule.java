package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import com.google.common.base.Optional;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.protocol.Codec;

public class NettyServerModule implements NetServerModule {

    public static NettyServerModule newInstance(
            RuntimeModule runtime) {
        Factory<? extends Publisher> publisherFactory = EventBusPublisher.factory();
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.getThreadFactory(), runtime.getServiceMonitor()));
        return newInstance(publisherFactory, bootstrapFactory);
    }
    
    public static NettyServerModule newInstance(
            Factory<? extends Publisher> publisherFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        return new NettyServerModule(publisherFactory, bootstrapFactory);
    }
    
    protected final Factory<? extends Publisher> publisherFactory;
    protected final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory;
    
    public NettyServerModule(
            Factory<? extends Publisher> publisherFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        this.publisherFactory = publisherFactory;
        this.bootstrapFactory = bootstrapFactory;
    }
    
    @Override
    public <I, T extends Codec<? super I,  ? extends Optional<?>>, C extends Connection<?>> ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<C>> getServerConnectionFactory(
            ParameterizedFactory<Publisher, Pair<Class<I>, T>> codecFactory,
            ParameterizedFactory<Pair<Pair<Class<I>, T>, Connection<I>>, C> connectionFactory) {
        ParameterizedFactory<Channel, C> factory = 
                ChannelCodecConnection.factory(publisherFactory, codecFactory, connectionFactory);
        return ChannelServerConnectionFactory.parameterizedFactory(publisherFactory, factory, bootstrapFactory);
    }
}
