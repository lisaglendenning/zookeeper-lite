package org.apache.zookeeper.netty.client;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.netty.ChannelConnection;
import org.apache.zookeeper.netty.ChannelConnectionGroup;
import org.apache.zookeeper.util.Eventful;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

public class ChannelClientConnectionGroup extends ChannelConnectionGroup implements ClientConnectionGroup {
    
    public static ChannelClientConnectionGroup create(
            Eventful eventful,
            ClientConnection.Factory connectionFactory,
            ChannelGroup channels,
            Bootstrap bootstrap) {
        return new ChannelClientConnectionGroup(eventful, connectionFactory, channels, bootstrap);
    }
    
    protected class ConnectListener implements ChannelFutureListener {
        protected SocketAddress remoteAddress;
        
        public ConnectListener(SocketAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }
        
        // called when connect() completes
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            SettableFuture<Connection> connectFuture = connectFutures.remove(remoteAddress);
            assert (connectFuture != null);
            if (future.isSuccess()) {
                ChannelConnection connection = ChannelClientConnectionGroup.super.initChannel(future.channel());
                connectFuture.set(connection);
            } else {
                if (future.isCancelled()) {
                    connectFuture.cancel(true);
                } else {
                    connectFuture.setException(future.cause());
                }
            }
        }
    }
    
    protected final Bootstrap bootstrap;
    protected final ConcurrentMap<SocketAddress, SettableFuture<Connection>> connectFutures;

    @Inject
    public ChannelClientConnectionGroup(
            Eventful eventful,
            ClientConnection.Factory connectionFactory,
            ChannelGroup channels,
            Bootstrap bootstrap) {
        super(eventful, connectionFactory, channels);
        this.bootstrap = checkNotNull(bootstrap);
        this.connectFutures = Maps.newConcurrentMap();
    }

    protected Bootstrap bootstrap() {
        return bootstrap;
    }
    
    @Override
    public ListenableFuture<Connection> connect(SocketAddress remoteAddress) {
        SettableFuture<Connection> future = connectFutures.putIfAbsent(remoteAddress, SettableFuture.<Connection>create());
        if (future == null) {
        	future = connectFutures.get(remoteAddress);
        	ChannelFuture channelFuture = bootstrap().connect(remoteAddress);
        	channelFuture.addListener(new ConnectListener(remoteAddress));
        }
        return future;
    }

    @Override
    protected ChannelConnection initChannel(Channel channel) {
        return null;
    }

    @Override
    protected void startUp() throws Exception {
        bootstrap().handler(new ChildInitializer());
        super.startUp();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        bootstrap().shutdown();
    }
}
