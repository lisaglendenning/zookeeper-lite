package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import com.google.common.collect.Sets;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;

import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class ChannelConnectionFactory<I, C extends Connection<I>> extends AbstractConnectionFactory<I,C> {

    public static enum ChannelGroupFactory implements DefaultsFactory<String, ChannelGroup> {
        INSTANCE;
        
        public static ChannelGroupFactory getInstance() {
            return INSTANCE;
        }
        
        @Override
        public ChannelGroup get() {
            return new DefaultChannelGroup();
        }

        @Override
        public ChannelGroup get(String name) {
            return new DefaultChannelGroup(name);
        }
    }
    
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
    
    private final ParameterizedFactory<Channel, C> connectionFactory;
    private final ChannelGroup channels;
    private final Set<C> connections;

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
    
    protected Set<C> connections() {
        return connections;
    }

    protected ParameterizedFactory<Channel, C> connectionFactory() {
        return connectionFactory;
    }

    protected C newChannel(Channel channel) {
        channels().add(channel);
        logger().trace("Added Channel: {}", channel);
        C connection = connectionFactory().get(channel);
        add(connection);
        return connection;
    }
    
    @Override
    protected boolean add(C connection) {
        connections().add(connection);
        return super.add(connection);
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
        // probably unnecessary?
        channels().close().await();
    }
    
    @Override
    public Iterator<C> iterator() {
        return connections().iterator();
    }
    
    @Override
    protected boolean remove(C connection) {
        return connections().remove(connection);
    }
}
