package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public enum ClientModule implements ParameterizedFactory<AbstractMain, Factory<? extends ChannelClientConnectionFactory>> {
    INSTANCE;
    
    public static ClientModule getInstance() {
        return INSTANCE;
    }
    
    @Override
    public Factory<? extends ChannelClientConnectionFactory> get(
            AbstractMain value) {
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(value.threadFactory(), value.serviceMonitor());
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.PerConnectionPublisherFactory.newInstance(value.publisherFactory());
        return ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(value.publisherFactory(), connectionBuilder, bootstrapFactory);
    }
}
