package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Sets;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class ChannelConnectionFactory<I, C extends Connection<I>> extends AbstractConnectionFactory<I,C> {
    
    protected static abstract class FactoryBuilder<I, C extends Connection<I>> {

        protected final Factory<Publisher> publisherFactory;
        protected final ParameterizedFactory<Channel, C> connectionFactory;
        
        protected FactoryBuilder(
                Factory<Publisher> publisherFactory,
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

    public ChannelGroup channels() {
        return channels;
    }
    
    protected C newChannel(Channel channel) {
        channels.add(channel);
        logger.trace("Added Channel: {}", channel);
        C connection = connectionFactory.get(channel);
        add(connection);
        return connection;
    }
    
    @Override
    protected boolean add(C connection) {
        connections.add(connection);
        return super.add(connection);
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        // probably unnecessary?
        channels.close().await();
    }
    
    @Override
    public Iterator<C> iterator() {
        return connections.iterator();
    }
    
    @Override
    protected boolean remove(C connection) {
        return connections.remove(connection);
    }
}
