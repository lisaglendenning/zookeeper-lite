package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import com.google.common.base.Optional;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.NetServerModule;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.netty.ChannelCodecConnection;
import edu.uw.zookeeper.protocol.Codec;

public class NettyServerModule implements NetServerModule {

    public static NettyServerModule newInstance(
            RuntimeModule runtime) {
        Factory<Publisher> publisherFactory = runtime.publisherFactory();
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor()));
        return newInstance(publisherFactory, bootstrapFactory);
    }
    
    public static NettyServerModule newInstance(
            Factory<Publisher> publisherFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        return new NettyServerModule(publisherFactory, bootstrapFactory);
    }
    
    protected final Factory<Publisher> publisherFactory;
    protected final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory;
    
    public NettyServerModule(
            Factory<Publisher> publisherFactory,
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
