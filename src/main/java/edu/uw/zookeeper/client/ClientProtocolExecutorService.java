package edu.uw.zookeeper.client;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated ClientProtocolExecutor in a Service.
 */
public class ClientProtocolExecutorService extends AbstractIdleService 
        implements Reference<ClientProtocolExecutor>, Publisher, ClientExecutor {

    public static ClientProtocolExecutorService newInstance(
            Factory<ClientProtocolExecutor> factory) {
        return new ClientProtocolExecutorService(factory);
    }
    
    protected final Factory<ClientProtocolExecutor> factory;
    protected volatile ClientProtocolExecutor client;
    
    protected ClientProtocolExecutorService(
            Factory<ClientProtocolExecutor> factory) {
        this.factory = factory;
        this.client = null;
    }
    
    protected Factory<ClientProtocolExecutor> factory() {
        return factory;
    }
    
    @Override
    protected void startUp() throws Exception {
        assert (client == null);
        this.client = factory().get();
        client.register(this);
        client.connect();
    }

    @Override
    protected void shutDown() throws Exception {
        if (client != null) {
            try {
                client.unregister(this);
            } catch (IllegalArgumentException e) {}
            
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
        
        return client;
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request) {
        State state = state();
        switch (state) {
        case NEW:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        return get().submit(request);
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        State state = state();
        switch (state) {
        case NEW:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        return get().submit(request, promise);
    }

    @Override
    public void post(Object object) {
        get().post(object);
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }
}
