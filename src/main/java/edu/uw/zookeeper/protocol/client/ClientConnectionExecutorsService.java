package edu.uw.zookeeper.protocol.client;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.DefaultsFactory;

/**
 * Wraps a ClientConnectionExecutor Factory in a Service.
 */
public class ClientConnectionExecutorsService<V, C extends Connection<Operation.Request>> extends AbstractIdleService implements DefaultsFactory<V, ClientConnectionExecutor<C>>, Iterable<ClientConnectionExecutor<C>> {

    public static <C extends Connection<Operation.Request>, V> ClientConnectionExecutorsService<V,C> newInstance(
            DefaultsFactory<V, ClientConnectionExecutor<C>> clientFactory) {
        return new ClientConnectionExecutorsService<V,C>(clientFactory);
    }
    
    protected final DefaultsFactory<V, ClientConnectionExecutor<C>> clientFactory;
    protected final ConcurrentMap<C, ClientConnectionExecutor<C>> handlers;
    
    protected ClientConnectionExecutorsService(
            DefaultsFactory<V, ClientConnectionExecutor<C>> clientFactory) {
        this.clientFactory = clientFactory;
        this.handlers = new MapMaker().makeMap();
    }
    
    public DefaultsFactory<V, ClientConnectionExecutor<C>> factory() {
        return clientFactory;
    }
    
    @Override
    public ClientConnectionExecutor<C> get() {
        State state = state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        ClientConnectionExecutor<C> handler = clientFactory.get();
        new ConnectionListener(handler);
        return handler;
    }

    @Override
    public ClientConnectionExecutor<C> get(V value) {
        State state = state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        ClientConnectionExecutor<C> handler = clientFactory.get(value);
        new ConnectionListener(handler);
        return handler;
    }
    
    @Override
    public Iterator<ClientConnectionExecutor<C>> iterator() {
        return handlers.values().iterator();
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        for (ClientConnectionExecutor<C> client: this) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
            try {
                DisconnectTask.<Void>create(null, client.get()).get();
            } finally {
                client.stop();
            }
        }
    }

    protected class ConnectionListener {
        
        protected final ClientConnectionExecutor<C> handler;
        
        public ConnectionListener(ClientConnectionExecutor<C> handler) {
            this.handler = handler;
            
            if (handlers.putIfAbsent(handler.get(), handler) != null) {
                throw new AssertionError();
            }
            handler.get().register(this);
        }
    
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    handler.get().unregister(this);
                } catch (IllegalArgumentException e) {}
                handlers.remove(handler.get(), handler);
            }
        }
    }
}
