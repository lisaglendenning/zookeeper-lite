package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public enum ServerModule implements ParameterizedFactory<RuntimeModule, ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory>> {
    INSTANCE;
    
    public static ServerModule getInstance() {
        return INSTANCE;
    }
    
    @Override
    public ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory> get(RuntimeModule runtime) {
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.PerConnectionPublisherFactory.newInstance(runtime.publisherFactory());
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor()));
        return ChannelServerConnectionFactory.ParameterizedServerFactoryBuilder.newInstance(runtime.publisherFactory(), connectionBuilder, bootstrapFactory);
    }
}
