package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public enum ServerModule implements ParameterizedFactory<AbstractMain, ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory>> {
    INSTANCE;
    
    public static ServerModule getInstance() {
        return INSTANCE;
    }
    
    @Override
    public ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory> get(AbstractMain main) {
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.PerConnectionPublisherFactory.newInstance(main.publisherFactory());
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(main.threadFactory(), main.serviceMonitor()));
        return ChannelServerConnectionFactory.ParameterizedServerFactoryBuilder.newInstance(main.publisherFactory(), connectionBuilder, bootstrapFactory);
    }
}
