package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ChannelServerConnectionFactory extends ChannelConnectionFactory
        implements ServerConnectionFactory {

    public static enum ChannelGroupFactory implements DefaultsFactory<String, ChannelGroup> {
        INSTANCE;
        
        public static Factory<ChannelGroup> getInstance() {
            return INSTANCE;
        }
        
        @Override
        public ChannelGroup get() {
            return ChannelConnectionFactory.newChannelGroup(ChannelServerConnectionFactory.class.getSimpleName());
        }

        @Override
        public ChannelGroup get(String name) {
            return ChannelConnectionFactory.newChannelGroup(name);
        }
    }
    
    public static class DefaultServerFactoryBuilder extends FactoryBuilder implements Factory<ChannelServerConnectionFactory> {

        public static Factory<ChannelServerConnectionFactory> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                Factory<ServerBootstrap> serverBootstrapFactory) {
            return new DefaultServerFactoryBuilder(publisherFactory, connectionFactory, serverBootstrapFactory);
        }
        
        private final Factory<ServerBootstrap> serverBootstrapFactory;
        
        private DefaultServerFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                Factory<ServerBootstrap> serverBootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.serverBootstrapFactory = serverBootstrapFactory;
        }

        @Override
        public ChannelServerConnectionFactory get() {
            return ChannelServerConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    serverBootstrapFactory.get());
        }
    }
    
    public static class ParameterizedServerFactoryBuilder extends FactoryBuilder implements ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> {

        public static ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory> create(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory) {
            return new ParameterizedServerFactoryBuilder(publisherFactory, connectionFactory, serverBootstrapFactory);
        }
        
        private final ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory;
        
        private ParameterizedServerFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
                ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.serverBootstrapFactory = serverBootstrapFactory;
        }

        @Override
        public ChannelServerConnectionFactory get(SocketAddress address) {
            return ChannelServerConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    serverBootstrapFactory.get(address));
        }
    }
    
    public static ChannelServerConnectionFactory newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
            ServerBootstrap bootstrap) {
        return new ChannelServerConnectionFactory(publisher, connectionFactory,
                bootstrap);
    }

    protected class CloseListener implements ChannelFutureListener {
        // called when serverChannel closes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            ChannelServerConnectionFactory.this.stop();
        }
    }

    private final ServerBootstrap bootstrap;
    private ServerChannel serverChannel;

    private ChannelServerConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
            ServerBootstrap bootstrap) {
        super(publisher, connectionFactory, ChannelGroupFactory.getInstance().get());
        this.bootstrap = checkNotNull(bootstrap).childHandler(new ChildInitializer());
        this.serverChannel = null;
    }

    public ServerChannel serverChannel() {
        return serverChannel;
    }

    protected ServerBootstrap serverBootstrap() {
        return bootstrap;
    }

    @Override
    public SocketAddress listenAddress() {
        SocketAddress listenAddress = null;
        if (serverChannel() != null) {
            listenAddress = serverChannel().localAddress();
        }
        return listenAddress;
    }

    @Override
    protected synchronized void startUp() throws Exception {
        assert (serverChannel == null);
        serverChannel = (ServerChannel) serverBootstrap().bind().sync().channel();
        logger().info("Listening on {}", serverChannel.localAddress());
        serverChannel().closeFuture().addListener(new CloseListener());
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        serverChannel().close().await();
        super.shutDown();
        // deprecated serverBootstrap().shutdown();
    }
}
