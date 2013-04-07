package org.apache.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.Connection;
import org.apache.zookeeper.ConnectionGroup;
import org.apache.zookeeper.event.ConnectionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

public abstract class AbstractConnectionGroup extends AbstractIdleService
        implements ConnectionGroup, Service {

    protected final Logger logger = LoggerFactory
            .getLogger(AbstractConnectionGroup.class);
    protected final ConcurrentMap<Pair<SocketAddress, SocketAddress>, Connection> connections;
    protected final Eventful eventful;

    @Inject
    public AbstractConnectionGroup(Eventful eventful) {
        this(
                eventful,
                Maps.<Pair<SocketAddress, SocketAddress>, Connection> newConcurrentMap());
    }

    protected AbstractConnectionGroup(
            Eventful eventful,
            ConcurrentMap<Pair<SocketAddress, SocketAddress>, Connection> connections) {
        this.eventful = checkNotNull(eventful);
        this.connections = checkNotNull(connections);
    }

    protected Eventful eventful() {
        return eventful;
    }

    protected ConcurrentMap<Pair<SocketAddress, SocketAddress>, Connection> connections() {
        return connections;
    }

    public Connection get(Pair<SocketAddress, SocketAddress> endpoints) {
        return connections().get(endpoints);
    }

    @Override
    public Iterator<Connection> iterator() {
        return connections().values().iterator();
    }

    protected Connection add(Connection connection) {
        logger.trace("Added Connection: {}", connection);
        Pair<SocketAddress, SocketAddress> endpoints = Pair.create(
                connection.localAddress(), connection.remoteAddress());
        Connection prev = connections().put(endpoints, connection);
        if (prev != null) {
            prev.close();
        }
        connection.register(this);
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
        List<ListenableFuture<Connection>> futures = Lists.newArrayList();
        for (Connection connection : this) {
            futures.add(connection.close());
        }
        ListenableFuture<List<Connection>> allFutures = Futures
                .allAsList(futures);
        allFutures.get();
    }

    @Subscribe
    public void handleConnectionEvent(ConnectionStateEvent event) {
        Connection connection = event.connection();
        switch (event.event()) {
        case CONNECTION_CLOSED:
            connections().remove(connection.remoteAddress(), connection);
            break;
        default:
            break;
        }
    }

    @Override
    public void post(Object event) {
        eventful().post(event);
    }

    @Override
    public void register(Object object) {
        eventful().register(object);
    }

    @Override
    public void unregister(Object object) {
        eventful().unregister(object);
    }
}
