package edu.uw.zookeeper.client;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated ClientProtocolExecutor in a Service.
 */
public class ClientProtocolExecutorService extends AbstractIdleService 
        implements Reference<ClientProtocolExecutor>, ClientExecutor {

    public static ClientProtocolExecutorService newInstance(
            Factory<ClientProtocolExecutor> clientFactory) {
        return new ClientProtocolExecutorService(clientFactory);
    }
    
    protected final Factories.SynchronizedLazyHolder<ClientProtocolExecutor> client;
    
    protected ClientProtocolExecutorService(
            Factory<ClientProtocolExecutor> clientFactory) {
        this.client = Factories.synchronizedLazyFrom(clientFactory);
    }
    
    @Override
    protected void startUp() throws Exception {
        ClientProtocolExecutor client = this.client.get();
        client.register(this);
        client.connect();
    }

    @Override
    protected void shutDown() throws Exception {
        if (this.client.has()) {
            ClientProtocolExecutor client = this.client.get();
            switch (client.state()) {
            case CONNECTING:
            case CONNECTED:
                client.disconnect().get();
                break;
            default:
                break;
            }
        }
    }

    @Override
    public ClientProtocolExecutor get() {
        State state = state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        
        return client.get();
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request) {
        return get().submit(request);
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        return get().submit(request, promise);
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
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
            try {
                unregister(this);
            } catch (IllegalArgumentException e) {}
            stop();
            break;
        default:
            break;
        }
    }
}
