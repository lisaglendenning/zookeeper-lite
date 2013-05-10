package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.util.Publisher;

public abstract class AbstractConnectionFactory extends AbstractIdleService
        implements ConnectionFactory, Service {

    private final Logger logger = LoggerFactory
            .getLogger(AbstractConnectionFactory.class);
    private final Publisher publisher;
    private final Set<Connection> connections;

    protected AbstractConnectionFactory(Publisher publisher) {
        this(publisher, Collections.synchronizedSet(Sets.<Connection>newHashSet()));
    }

    protected AbstractConnectionFactory(
            Publisher publisher,
            Set<Connection> connections) {
        super();
        this.publisher = checkNotNull(publisher);
        this.connections = checkNotNull(connections);
    }

    protected Publisher publisher() {
        return publisher;
    }

    protected Set<Connection> connections() {
        return connections;
    }
    
    protected Logger logger() {
        return logger;
    }

    @Override
    public Iterator<Connection> iterator() {
        return connections().iterator();
    }

    protected Connection add(Connection connection) {
        checkState(state() == State.RUNNING);
        logger().trace("Added Connection: {}", connection);
        boolean added = connections().add(connection);
        assert(added);
        connection.register(this);
        try {
            post(NewConnectionEvent.create(connection));
        } catch (Exception e) {
            connection.close();
            connections().remove(connection);
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
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            connections().remove(connection);
            break;
        default:
            break;
        }
    }

    protected void post(Object event) {
        publisher().post(event);
    }

    @Override
    public void register(Object object) {
        publisher().register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher().unregister(object);
    }
}
