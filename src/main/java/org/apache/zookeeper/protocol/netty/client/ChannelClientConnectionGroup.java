package org.apache.zookeeper.protocol.netty.client;

import static com.google.common.base.Preconditions.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.client.ClientConnectionGroup;
import org.apache.zookeeper.protocol.netty.ChannelConnection;
import org.apache.zookeeper.protocol.netty.ChannelConnectionGroup;
import org.apache.zookeeper.util.Eventful;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ChannelClientConnectionGroup extends ChannelConnectionGroup implements ClientConnectionGroup {
    
    public static ChannelClientConnectionGroup create(
            Eventful eventful,
            Provider<ClientConnection> connectionFactory,
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
            if (connectFuture == null) {
                if (future.channel() != null) {
                    future.channel().close();
                }
                return;
            }
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
    protected final Map<SocketAddress, SettableFuture<Connection>> connectFutures;

    @Inject
    public ChannelClientConnectionGroup(
            Eventful eventful,
            Provider<ClientConnection> connectionFactory,
            ChannelGroup channels,
            Bootstrap bootstrap) {
        super(eventful, connectionFactory, channels);
        this.bootstrap = checkNotNull(bootstrap);
        this.connectFutures = Collections.synchronizedMap(Maps.<SocketAddress, SettableFuture<Connection>>newHashMap());
    }

    protected Bootstrap bootstrap() {
        return bootstrap;
    }
    
    @Override
    public ListenableFuture<Connection> connect(SocketAddress remoteAddress) {
        checkState(get(remoteAddress) == null);
        SettableFuture<Connection> future = null;
        synchronized (connectFutures) {
            future = connectFutures.get(remoteAddress);
            if (future != null) {
                return future;
            } else {
                future = SettableFuture.create();
                connectFutures.put(remoteAddress, future);
            }
        }
        ChannelFuture channelFuture = bootstrap().connect(remoteAddress);
        channelFuture.addListener(new ConnectListener(remoteAddress));
        return future;
    }

    @Override
    protected ChannelConnection initChannel(Channel channel) {
        return null;
/*        ChannelConnection connection = super.initChannel(channel);
        SettableFuture<Connection> future = connectFutures.remove(channel.remoteAddress());
        assert (future != null);
        future.set(connection);
        return connection;*/
    }

    @Override
    protected void startUp() throws Exception {
        bootstrap().handler(new ChildInitializer());
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        bootstrap().shutdown();
    }
}
