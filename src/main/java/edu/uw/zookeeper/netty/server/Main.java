package edu.uw.zookeeper.netty.server;

import java.net.SocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.netty.ChannelServerConnectionFactory;
import edu.uw.zookeeper.server.ServerMain;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class Main extends ServerMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(Main.class));
    }
    
    protected final ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> serverConnectionFactory;
    
    public Main(Configuration configuration) {
        super(configuration);
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.ConnectionBuilder.newInstance(publisherFactory());
        ParameterizedFactory<SocketAddress, ServerBootstrap> bootstrapFactory = 
                NioServerBootstrapFactory.ParameterizedDecorator.newInstance(
                        NioServerBootstrapFactory.newInstance(threadFactory(), serviceMonitor()));
        this.serverConnectionFactory = ChannelServerConnectionFactory.ParameterizedServerFactoryBuilder.newInstance(publisherFactory(), connectionBuilder, bootstrapFactory);
    }

    @Override
    protected ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> serverConnectionFactory() {
        return serverConnectionFactory;
    }
}
