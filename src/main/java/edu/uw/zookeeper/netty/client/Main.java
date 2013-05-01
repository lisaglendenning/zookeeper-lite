package edu.uw.zookeeper.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import edu.uw.zookeeper.client.ClientMain;
import edu.uw.zookeeper.netty.ChannelClientConnectionFactory;
import edu.uw.zookeeper.netty.ChannelConnection;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;

public class Main extends ClientMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(Main.class));
    }

    protected final Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory;
    
    public Main(Configuration configuration) {
        super(configuration);
        Factory<Bootstrap> bootstrapFactory = NioClientBootstrapFactory.newInstance(threadFactory(), serviceMonitor());
        ParameterizedFactory<Channel, ChannelConnection> connectionBuilder = ChannelConnection.ConnectionBuilder.newInstance(publisherFactory());
        this.clientConnectionFactory = 
                ChannelClientConnectionFactory.ClientFactoryBuilder.newInstance(publisherFactory(), connectionBuilder, bootstrapFactory);
    }

    @Override
    protected Factory<? extends ChannelClientConnectionFactory> clientConnectionFactory() {
        return clientConnectionFactory;
    }
}
