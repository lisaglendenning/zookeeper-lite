package org.apache.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import java.util.Iterator;
import org.apache.zookeeper.AbstractConnectionGroup;
import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionGroup;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

public class ChannelConnectionGroup extends AbstractConnectionGroup implements ConnectionGroup, Service {

    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        public ChildInitializer() {}
        
        @Override
        public void initChannel(Channel channel) throws Exception {
            ChannelConnectionGroup.this.initChannel(channel);
        }
    }
    
    protected final Logger logger = LoggerFactory.getLogger(ChannelConnectionGroup.class);
    protected final ChannelGroup channels;
    protected final ChannelConnection.Factory<? extends ChannelConnection> connectionFactory;

    @Inject
    public ChannelConnectionGroup(
            Eventful eventful,
            ChannelConnection.Factory<? extends ChannelConnection> connectionFactory,
            ChannelGroup channels) {
    	super(eventful);
        this.connectionFactory = checkNotNull(connectionFactory);
        this.channels = checkNotNull(channels);
    }

    protected ChannelGroup channels() {
        return channels;
    }
    
    protected ChannelConnection.Factory<? extends ChannelConnection> connectionFactory() {
        return connectionFactory;
    }
    
    @Override
    public Connection get(SocketAddress remoteAddress) {
        return connections().get(remoteAddress);
    }

    @Override
    public Iterator<Connection> iterator() {
        return connections().values().iterator();
    }
    
    protected ChannelConnection initChannel(Channel channel) {
    	checkState(state() == State.RUNNING);
        logger.trace("New Channel: {}", channel);
        channels().add(channel);
        ChannelConnection connection = connectionFactory().get(channel);
        add(connection);
        return connection;
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        // probably unnecessary...
        channels().close().await();
    }
}
