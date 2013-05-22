package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public enum ClientModule implements ParameterizedFactory<RuntimeModule, Factory<? extends ChannelClientConnectionFactory>> {
    INSTANCE;
    
    public static ClientModule getInstance() {
        return INSTANCE;
    }
    
    @Override
    public Factory<? extends ChannelClientConnectionFactory> get(
            RuntimeModule runtime) {
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(runtime.threadFactory(), runtime.serviceMonitor());
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.PerConnectionPublisherFactory.newInstance(runtime.publisherFactory());
        return ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(runtime.publisherFactory(), connectionBuilder, bootstrapFactory);
    }
}
