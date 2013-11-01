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

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;
import edu.uw.zookeeper.net.ServerConnectionFactory;

public class ChannelServerConnectionFactory<C extends Connection<?,?,?>> 
        extends ChannelConnectionFactory<C>
        implements ServerConnectionFactory<C> {

    public static <C extends Connection<?,?,?>> ParameterizedFactory<SocketAddress, ? extends ChannelServerConnectionFactory<C>> factory(
            final ParameterizedFactory<Channel, C> connectionFactory,
            final ParameterizedFactory<SocketAddress, ? extends ServerBootstrap> bootstrapFactory) {
        return new ParameterizedFactory<SocketAddress, ChannelServerConnectionFactory<C>>() {
            @Override
            public ChannelServerConnectionFactory<C> get(SocketAddress address) {
                return ChannelServerConnectionFactory.newInstance(
                        connectionFactory, 
                        bootstrapFactory.get(address));
            }
        };
    }

    public static <C extends Connection<?,?,?>> ChannelServerConnectionFactory<C> newInstance(
            ParameterizedFactory<Channel, C> connectionFactory,
            ServerBootstrap bootstrap) {
        IConcurrentSet<ConnectionsListener<? super C>> listeners = new StrongConcurrentSet<ConnectionsListener<? super C>>();
        ChannelGroup channels = new DefaultChannelGroup(ChannelServerConnectionFactory.class.getSimpleName(), bootstrap.childGroup().next());
        return new ChannelServerConnectionFactory<C>(
                listeners, 
                connectionFactory,
                channels,
                bootstrap);
    }
    
    protected final ServerBootstrap bootstrap;
    protected volatile ServerChannel serverChannel;

    protected ChannelServerConnectionFactory(
            IConcurrentSet<ConnectionsListener<? super C>> listeners,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup group,
            ServerBootstrap bootstrap) {
        super(listeners, connectionFactory, group);
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
        checkState(isRunning());
        assert (serverChannel != null);
        return serverChannel.localAddress();
    }

    @Override
    protected void startUp() throws Exception {
        assert (serverChannel == null);
        serverChannel = (ServerChannel) serverBootstrap().bind().sync().channel();
        logger.info(LoggingMarker.NET_MARKER.get(), "Server listening to {}", serverChannel.localAddress());
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
            ChannelServerConnectionFactory.this.stopAsync();
        }
    }
}
