package org.apache.zookeeper.netty.protocol;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionEventValue;
import org.apache.zookeeper.ConnectionGroup;
import org.apache.zookeeper.ConnectionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ChannelConnectionGroup extends AbstractIdleService implements ConnectionGroup, Service {

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
    protected final Provider<? extends ChannelConnection> connectionFactory;
    protected final Map<SocketAddress, Connection> connections;
    protected final Eventful eventful;

    @Inject
    public ChannelConnectionGroup(
            Eventful eventful,
            Provider<? extends ChannelConnection> connectionFactory,
            ChannelGroup channels) {
        this.eventful = checkNotNull(eventful);
        this.connectionFactory = checkNotNull(connectionFactory);
        this.channels = checkNotNull(channels);
        this.connections = Collections.synchronizedMap(Maps.<SocketAddress, Connection>newHashMap());
    }

    protected Map<SocketAddress, Connection> connections() {
        return connections;
    }
    
    protected ChannelGroup channels() {
        return channels;
    }
    
    protected Provider<? extends ChannelConnection> connectionFactory() {
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
        logger.trace("New Channel: {}", channel);
        channels().add(channel);
        SocketAddress remoteAddress = channel.remoteAddress();
        ChannelConnection connection = connectionFactory().get();
        connections().put(remoteAddress, connection);
        connection.register(this);
        connection.attach(channel);
        try {
            post(connection);
        } catch (Exception e) {
            connection.close();
        }
        return connection;
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        channels().close().await();
    }

    @Subscribe
    public void handleConnectionEvent(ConnectionEventValue<?> event) {
        // TODO
    }

    @Subscribe
    public void handleConnectionEvent(ConnectionStateEvent event) {
        Connection connection = event.connection();
        switch (event.event()) {
        case CLOSED:
            synchronized (connections()) {
                if (connections().get(connection.remoteAddress()) == connection) {
                    connections().remove(connection.remoteAddress());
                }
            }
            break;
        default:
            break;
        }
    }

    @Override
    public void post(Object event) {
        eventful.post(event);
    }

    @Override
    public void register(Object object) {
        eventful.register(object);
    }

    @Override
    public void unregister(Object object) {
        eventful.unregister(object);
    }
}
