package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.engio.mbassy.PubSubSupport;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.References;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;

public abstract class AbstractConnectionFactory<C extends Connection<?>> extends AbstractIdleService
        implements ConnectionFactory<C> {

    protected final Logger logger;
    protected final PubSubSupport<Object> publisher;

    protected AbstractConnectionFactory(PubSubSupport<Object> publisher) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.publisher = checkNotNull(publisher);
    }

    @Override
    public void publish(Object event) {
        publisher.publish(event);
    }

    @Override
    public void subscribe(Object listener) {
        publisher.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return publisher.unsubscribe(listener);
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
                        logger.trace(LoggingMarker.NET_MARKER.get(), "ADDED {}", connection);
                        publish(connection);
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
            logger.trace(LoggingMarker.NET_MARKER.get(), "REMOVED {}", connection);
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
    
    @net.engio.mbassy.listener.Listener(references = References.Strong)
    protected class RemoveConnectionOnClose {
    
        protected final C connection;
        
        public RemoveConnectionOnClose(C connection) {
            this.connection = connection;
            connection.subscribe(this);
        }
        
        @Handler
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    connection.unsubscribe(this);
                } catch (Exception e) {}
                remove(connection);
            }
        }
    }
}
