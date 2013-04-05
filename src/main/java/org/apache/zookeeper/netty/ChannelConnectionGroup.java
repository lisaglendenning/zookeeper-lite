package org.apache.zookeeper.netty;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;

import org.apache.zookeeper.AbstractConnectionGroup;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class ChannelConnectionGroup extends AbstractConnectionGroup {

    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        public ChildInitializer() {}
        
        @Override
        public void initChannel(Channel channel) throws Exception {
            ChannelConnectionGroup.this.add(channel);
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
    
    protected ChannelConnection add(Channel channel) {
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
