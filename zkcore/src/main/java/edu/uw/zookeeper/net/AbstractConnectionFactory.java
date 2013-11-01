package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.engio.mbassy.common.IConcurrentSet;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;

public abstract class AbstractConnectionFactory<C extends Connection<?,?,?>> extends AbstractIdleService
        implements ConnectionFactory<C> {
    
    protected final Logger logger;
    protected final IConcurrentSet<ConnectionsListener<? super C>> listeners;

    protected AbstractConnectionFactory(
            IConcurrentSet<ConnectionsListener<? super C>> listeners) {
        this.logger = LogManager.getLogger(getClass());
        this.listeners = checkNotNull(listeners);
    }

    @Override
    public void subscribe(ConnectionsListener<? super C> listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(ConnectionsListener<? super C> listener) {
        return listeners.remove(listener);
    }
    
    @Override
    public Iterator<C> iterator() {
        return Iterators.transform(
                connections().iterator(), 
                new Function<ConnectionListener, C>() {
                    @Override
                    public C apply(ConnectionListener input) {
                        return input.get();
                    }
        });
    }

    protected boolean add(C connection) {
        if (isRunning()) {
            ConnectionListener listener = new ConnectionListener(connection);
            if (connections().add(listener)) {
                if (isRunning()) {
                    logger.trace(LoggingMarker.NET_MARKER.get(), "ADDED {}", connection);
                    for (ConnectionsListener<? super C> l: listeners) {
                        l.handleConnectionOpen(connection);
                    }
                    return true;
                } else {
                    remove(listener);
                }
            }
        }
        connection.close();
        return false;
    }
    
    protected boolean remove(ConnectionListener listener) {
        boolean removed = connections().remove(listener);
        if (removed) {
            logger.trace(LoggingMarker.NET_MARKER.get(), "REMOVED {}", listener);
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
            Iterator<ConnectionListener> itr = Iterators.consumingIterator(connections().iterator());
            while (itr.hasNext()) {
                C next = itr.next().get();
                if (next.state() != Connection.State.CONNECTION_CLOSED) {
                    futures.add(next.close());
                }
            }
        }
        Futures.successfulAsList(futures).get();

        Iterator<?> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            itr.next();
        }
    }

    protected abstract Collection<ConnectionListener> connections();
    
    protected class ConnectionListener extends Factories.Holder<C> implements Connection.Listener<Object> {
    
        public ConnectionListener(C connection) {
            super(checkNotNull(connection));
            connection.subscribe(this);
            if (Connection.State.CONNECTION_CLOSED == connection.state()) {
                handleConnectionState(Automaton.Transition.create(Connection.State.CONNECTION_OPENING, Connection.State.CONNECTION_CLOSED));
            }
        }
        
        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> transition) {
            if (Connection.State.CONNECTION_CLOSED == transition.to()) {
                get().unsubscribe(this);
                remove(this);
            }
        }

        @Override
        public void handleConnectionRead(Object message) {
        }
    }
}
