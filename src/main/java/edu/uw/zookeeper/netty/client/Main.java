package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;

import edu.uw.zookeeper.client.ClientMain;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.LazyHolder;
import edu.uw.zookeeper.util.MonitoredService;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Singleton;

public class Main extends ClientMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(Main.class));
    }
    
    protected final Singleton<ChannelClientConnectionFactory> connections;
    
    public Main(Configuration configuration) {
        super(configuration);
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(threadFactory(), serviceMonitor());
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.ConnectionBuilder.newInstance(publisherFactory());
        Factory<ChannelClientConnectionFactory> clientFactory = 
                MonitoredService.newInstance(
                ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(publisherFactory(), connectionBuilder, bootstrapFactory),
                serviceMonitor());
        
        this.connections = LazyHolder.newInstance(clientFactory);
    }

    protected ClientConnectionFactory connections() {
        return connections.get();
    }
}
