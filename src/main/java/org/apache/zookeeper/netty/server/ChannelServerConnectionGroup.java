package org.apache.zookeeper.netty.server;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;

import org.apache.zookeeper.netty.ChannelConnectionGroup;
import org.apache.zookeeper.server.ServerConnectionGroup;
import org.apache.zookeeper.util.Eventful;

import com.google.inject.Inject;

public class ChannelServerConnectionGroup extends ChannelConnectionGroup
        implements ServerConnectionGroup {

    public static ChannelServerConnectionGroup create(Eventful eventful,
            ServerConnection.Factory connectionFactory, ChannelGroup channels,
            ServerBootstrap bootstrap) {
        return new ChannelServerConnectionGroup(eventful, connectionFactory,
                channels, bootstrap);
    }

    protected class CloseListener implements ChannelFutureListener {
        // called when serverChannel closes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            ChannelServerConnectionGroup.this.stop();
        }
    }

    protected final ServerBootstrap bootstrap;
    protected ServerChannel serverChannel;

    @Inject
    public ChannelServerConnectionGroup(Eventful eventful,
            ServerConnection.Factory connectionFactory, ChannelGroup channels,
            ServerBootstrap bootstrap) {
        super(eventful, connectionFactory, channels);
        this.bootstrap = checkNotNull(bootstrap);
        this.serverChannel = null;
    }

    public ServerChannel serverChannel() {
        return serverChannel;
    }

    protected ServerBootstrap serverBootstrap() {
        return bootstrap;
    }

    @Override
    public SocketAddress localAddress() {
        SocketAddress socketAddress = null;
        if (serverChannel() != null) {
            socketAddress = serverChannel().localAddress();
        }
        return socketAddress;
    }

    @Override
    protected void startUp() throws Exception {
        serverChannel = (ServerChannel) serverBootstrap()
                .childHandler(new ChildInitializer()).bind().sync().channel();
        logger.info("Listening on {}", serverChannel.localAddress());
        serverChannel().closeFuture().addListener(new CloseListener());
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        serverChannel().close().await();
        super.shutDown();
        serverBootstrap().shutdown();
    }
}
