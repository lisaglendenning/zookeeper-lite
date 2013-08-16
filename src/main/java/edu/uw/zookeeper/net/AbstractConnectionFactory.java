package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
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
    public UnmodifiableIterator<C> iterator() {
        return ImmutableSet.copyOf(connections()).iterator();
    }

    protected boolean add(C connection) {
        if (isRunning()) {
            if (connections().add(connection)) {
                if (isRunning()) {
                    RemoveConnectionOnClose listener = new RemoveConnectionOnClose(connection);
                    if (Connection.State.CONNECTION_CLOSED == connection.state()) {
                        listener.handleStateEvent(Automaton.Transition.create(Connection.State.CONNECTION_OPENING, Connection.State.CONNECTION_CLOSED));
                    } else {
                        logger.trace(Logging.NET_MARKER, "ADDED {}", connection);
                        post(connection);
                        return true;
                    }
                } else {
                    remove(connection);
                }
            }
        }
        connection.close();
        return false;
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
        List<ListenableFuture<?>> futures = Lists.newLinkedList();
        synchronized (connections()) {
            Iterator<C> itr = Iterators.consumingIterator(connections().iterator());
            while (itr.hasNext()) {
                C next = itr.next();
                if (next.state() != Connection.State.CONNECTION_CLOSED) {
                    futures.add(next.close());
                }
            }
        }
        Futures.successfulAsList(futures).get();
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
                } catch (Exception e) {}
                remove(connection);
            }
        }
    }
}
