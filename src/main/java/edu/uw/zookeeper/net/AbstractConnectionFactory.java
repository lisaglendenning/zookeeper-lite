package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;

public abstract class AbstractConnectionFactory<C extends Connection<?>> extends AbstractIdleService
        implements ConnectionFactory<C>, Service {

    protected final Logger logger;
    protected final Publisher publisher;

    protected AbstractConnectionFactory(Publisher publisher) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.publisher = checkNotNull(publisher);
    }

    @Override
    public void post(Object event) {
        publisher.post(event);
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }
    
    @Override
    public Iterator<C> iterator() {
        return connections().iterator();
    }

    protected boolean add(C connection) {
        boolean added = connections().add(connection);
        if (added) {
            new RemoveConnectionOnClose(connection);
            State state = state();
            if (state != State.RUNNING) {
                connection.close();
                throw new IllegalStateException(state.toString());
            }
            logger.trace(Logging.NET_MARKER, "ADDED {}", connection);
            post(connection);
        }
        return added;
    }
    
    protected boolean remove(C connection) {
        boolean removed = connections().remove(connection);
        if (removed) {
            logger.trace(Logging.NET_MARKER, "REMOVED {}", connection);
        }
        return removed;
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (C connection : this) {
            futures.add(connection.close());
        }
        ListenableFuture<List<Object>> allFutures = Futures
                .allAsList(futures);
        allFutures.get();
    }

    protected abstract Collection<C> connections();

    protected class RemoveConnectionOnClose {
    
        protected final C connection;
        
        public RemoveConnectionOnClose(C connection) {
            this.connection = connection;
            connection.register(this);
        }
        
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
                remove(connection);
            }
        }
    }
}
