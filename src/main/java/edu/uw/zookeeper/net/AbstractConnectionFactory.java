package edu.uw.zookeeper.net;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionFactory;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Publisher;

public abstract class AbstractConnectionFactory<I, C extends Connection<I>> extends AbstractIdleService
        implements ConnectionFactory<I,C>, Service {

    protected final Logger logger;
    protected final Publisher publisher;

    protected AbstractConnectionFactory(Publisher publisher) {
        super();
        this.logger = LoggerFactory.getLogger(getClass());
        this.publisher = checkNotNull(publisher);
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    protected boolean add(C connection) {
        new RemoveConnectionOnClose(connection);
        State state = state();
        if (state != State.RUNNING) {
            connection.close();
            throw new IllegalStateException(state.toString());
        }
        post(connection);
        logger.trace("Added Connection: {}", connection);
        return true;
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        List<ListenableFuture<Connection<I>>> futures = Lists.newArrayList();
        for (Connection<I> connection : this) {
            futures.add(connection.close());
        }
        ListenableFuture<List<Connection<I>>> allFutures = Futures
                .allAsList(futures);
        allFutures.get();
    }

    protected void post(Object event) {
        publisher.post(event);
    }

    protected abstract boolean remove(C connection);

    protected class RemoveConnectionOnClose {
    
        protected final C connection;
        
        public RemoveConnectionOnClose(C connection) {
            this.connection = connection;
            connection.register(this);
        }
        
        public void close() {
            remove(connection);
            try {
                connection.unregister(this);
            } catch (IllegalArgumentException e) {}
        }
    
        @SuppressWarnings("unchecked")
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (event.type().isAssignableFrom(Connection.State.class)) {
                handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
            }
        }
        
        public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
            switch (event.to()) {
            case CONNECTION_CLOSED:
                close();
                break;
            default:
                break;
            }
        }
    }
}
