package edu.uw.zookeeper.netty;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import net.engio.mbassy.common.IConcurrentSet;

import com.google.common.collect.Sets;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.AbstractConnectionFactory;
import edu.uw.zookeeper.net.Connection;

public abstract class ChannelConnectionFactory<C extends Connection<?,?,?>> extends AbstractConnectionFactory<C> {
    
    protected final ParameterizedFactory<Channel, C> connectionFactory;
    protected final ChannelGroup channels;
    protected final Set<ConnectionListener> connections;

    protected ChannelConnectionFactory(
            IConcurrentSet<ConnectionsListener<? super C>> listeners,
            ParameterizedFactory<Channel, C> connectionFactory,
            ChannelGroup channels) {
        super(listeners);
        this.connectionFactory = checkNotNull(connectionFactory);
        this.channels = checkNotNull(channels);
        this.connections = Collections.synchronizedSet(Sets.<ConnectionListener>newHashSet());
    }

    protected ChannelGroup channels() {
        return channels;
    }
    
    @Override
    protected Collection<ConnectionListener> connections() {
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
