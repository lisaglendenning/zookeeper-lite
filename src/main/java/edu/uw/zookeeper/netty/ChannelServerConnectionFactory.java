package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import java.net.SocketAddress;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ServerConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public class ChannelServerConnectionFactory<I, C extends Connection<I>> 
        extends ChannelConnectionFactory<I, C>
        implements ServerConnectionFactory<I, C> {
    
    public static <I, C extends Connection<I>> DefaultServerFactoryBuilder<I,C> defaultsFactory(
            Factory<Publisher> publisherFactory,
            ParameterizedFactory<Channel, C> connectionFactory,
            Factory<ServerBootstrap> serverBootstrapFactory) {
        return DefaultServerFactoryBuilder.newInstance(
                publisherFactory, connectionFactory, serverBootstrapFactory);
    }
    
    public static <I, C extends Connection<I>> ParameterizedServerFactoryBuilder<I,C> parameterizedFactory(
            Factory<Publisher> publisherFactory,
            ParameterizedFactory<Channel, C> connectionFactory,
            ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory) {
        return ParameterizedServerFactoryBuilder.newInstance(
                publisherFactory, connectionFactory, serverBootstrapFactory);
    }

    public static class DefaultServerFactoryBuilder<I, C extends Connection<I>> extends FactoryBuilder<I,C> implements Factory<ChannelServerConnectionFactory<I,C>> {

        public static <I, C extends Connection<I>> DefaultServerFactoryBuilder<I,C> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<ServerBootstrap> serverBootstrapFactory) {
            return new DefaultServerFactoryBuilder<I,C>(
                    publisherFactory, connectionFactory, serverBootstrapFactory);
        }
        
        private final Factory<ServerBootstrap> serverBootstrapFactory;
        
        private DefaultServerFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                Factory<ServerBootstrap> serverBootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.serverBootstrapFactory = serverBootstrapFactory;
        }

        @Override
        public ChannelServerConnectionFactory<I,C> get() {
            return ChannelServerConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    serverBootstrapFactory.get());
        }
    }
    
    public static class ParameterizedServerFactoryBuilder<I, C extends Connection<I>> extends FactoryBuilder<I,C> implements ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<I,C>> {

        public static <I, C extends Connection<I>> ParameterizedServerFactoryBuilder<I,C> newInstance(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory) {
            return new ParameterizedServerFactoryBuilder<I,C>(
                    publisherFactory, connectionFactory, serverBootstrapFactory);
        }
        
        private final ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory;
        
        private ParameterizedServerFactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory,
                ParameterizedFactory<SocketAddress, ServerBootstrap> serverBootstrapFactory) {
            super(publisherFactory, connectionFactory);
            this.serverBootstrapFactory = serverBootstrapFactory;
        }

        @Override
        public ChannelServerConnectionFactory<I,C> get(SocketAddress address) {
            return ChannelServerConnectionFactory.newInstance(
                    publisherFactory.get(), 
                    connectionFactory, 
                    serverBootstrapFactory.get(address));
        }
    }

    public static <I, C extends Connection<I>> ChannelServerConnectionFactory<I,C> newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ServerBootstrap bootstrap) {
        ChannelGroup channels = new DefaultChannelGroup(ChannelServerConnectionFactory.class.getSimpleName(), bootstrap.childGroup().next());
        return newInstance(
                publisher, 
                connectionFactory,
                channels,
                bootstrap);
    }
    
    public static <I, C extends Connection<I>> ChannelServerConnectionFactory<I,C> newInstance(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup channels,
            ServerBootstrap bootstrap) {
        return new ChannelServerConnectionFactory<I,C>(
                publisher, 
                connectionFactory,
                channels,
                bootstrap);
    }

    protected final ServerBootstrap bootstrap;
    protected volatile ServerChannel serverChannel;

    protected ChannelServerConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup group,
            ServerBootstrap bootstrap) {
        super(publisher, connectionFactory, group);
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
        // unfortunately we can't just ask the bootstrap for our address...
        SocketAddress listenAddress = null;
        if (serverChannel != null) {
            listenAddress = serverChannel.localAddress();
        }
        return listenAddress;
    }

    @Override
    protected void startUp() throws Exception {
        assert (serverChannel == null);
        serverChannel = (ServerChannel) serverBootstrap().bind().sync().channel();
        logger.info("Listening on {}", serverChannel.localAddress());
        serverChannel.closeFuture().addListener(new CloseListener());
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        if (serverChannel != null) {
            serverChannel.close().await();
        }
        super.shutDown();
    }

    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        
        public ChildInitializer() {
        }
    
        @Override
        public void initChannel(Channel channel) throws Exception {
            newChannel(channel);
        }
    }

    protected class CloseListener implements ChannelFutureListener {
        // called when serverChannel closes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            ChannelServerConnectionFactory.this.stop();
        }
    }
}
