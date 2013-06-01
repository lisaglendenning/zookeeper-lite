package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class ServerModule {

    public static <I,O, C extends Connection<I>> ParameterizedFactory<Connection.CodecFactory<I,O,C>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<I,C>>> factory(
            final RuntimeModule runtime) {
        final Factory<Publisher> publisherFactory = runtime.publisherFactory();
        final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor()));
        return factory(publisherFactory, bootstrapFactory);
    }
    
    public static <I,O, C extends Connection<I>> ParameterizedFactory<Connection.CodecFactory<I,O,C>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<I,C>>> factory(
            final Factory<Publisher> publisherFactory,
            final ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory) {
        return new ParameterizedFactory<Connection.CodecFactory<I,O,C>, ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<I,C>>>() {
            @Override
            public ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<I,C>> get(Connection.CodecFactory<I,O,C> value) {
                ParameterizedFactory<Channel, C> connectionFactory = 
                        ChannelConnection.factory(publisherFactory, value);
                return ChannelServerConnectionFactory.parameterizedFactory(publisherFactory, connectionFactory, bootstrapFactory);
            }
        };
    }
}
