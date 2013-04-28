package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class ChannelConnectionFactory extends AbstractConnectionFactory {
    
    protected static abstract class FactoryBuilder {

        protected final Factory<Publisher> publisherFactory;
        protected final ParameterizedFactory<Channel, ChannelConnection> connectionFactory;
        
        protected FactoryBuilder(
                Factory<Publisher> publisherFactory,
                ParameterizedFactory<Channel, ChannelConnection> connectionFactory) {
            super();
            this.publisherFactory = publisherFactory;
            this.connectionFactory = connectionFactory;
        }
    }
    
    protected static ChannelGroup newChannelGroup(String name) {
        return new DefaultChannelGroup(name);
    }
    
    @ChannelHandler.Sharable
    protected class ChildInitializer extends ChannelInitializer<Channel> {
        public ChildInitializer() {
        }

        @Override
        public void initChannel(Channel channel) throws Exception {
            ChannelConnectionFactory.this.add(channel);
        }
    }

    private final ParameterizedFactory<Channel, ChannelConnection> connectionFactory;
    private final ChannelGroup channels;

    protected ChannelConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, ChannelConnection> connectionFactory,
            ChannelGroup channels) {
        super(publisher);
        this.connectionFactory = checkNotNull(connectionFactory);
        this.channels = checkNotNull(channels);
    }

    protected ChannelGroup channels() {
        return channels;
    }

    protected ParameterizedFactory<Channel, ChannelConnection> connectionFactory() {
        return connectionFactory;
    }

    protected ChannelConnection add(Channel channel) {
        checkState(state() == State.RUNNING);
        logger().trace("Added Channel: {}", channel);
        channels().add(channel);
        ChannelConnection connection = connectionFactory().get(channel);
        add(connection);
        return connection;
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        // probably unnecessary?
        channels().close().await();
    }
}
