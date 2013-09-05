package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import com.google.common.collect.Sets;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public abstract class ChannelConnectionFactory<C extends Connection<?>> extends AbstractConnectionFactory<C> {
    
    protected static abstract class FactoryBuilder<C extends Connection<?>> {

        protected final Factory<? extends Publisher> publisherFactory;
        protected final ParameterizedFactory<Channel, C> connectionFactory;
        
        protected FactoryBuilder(
                Factory<? extends Publisher> publisherFactory,
                ParameterizedFactory<Channel, C> connectionFactory) {
            super();
            this.publisherFactory = publisherFactory;
            this.connectionFactory = connectionFactory;
        }
    }
    
    protected final ParameterizedFactory<Channel, C> connectionFactory;
    protected final ChannelGroup channels;
    protected final Set<C> connections;

    protected ChannelConnectionFactory(
            Publisher publisher,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup channels) {
        super(publisher);
        this.connectionFactory = checkNotNull(connectionFactory);
        this.channels = checkNotNull(channels);
        this.connections = Collections.synchronizedSet(Sets.<C>newHashSet());
    }

    protected ChannelGroup channels() {
        return channels;
    }
    
    @Override
    protected Collection<C> connections() {
        return connections;
    }

    protected C newChannel(Channel channel) {
        if (channels.add(channel)) {
            C connection = connectionFactory.get(channel);
            if (add(connection)) {
                return connection;
            }
        }
        channel.close();
        return null;
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        // should be unnecessary...
        channels.close().await();
    }
}
