package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection.RequestFuture;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated ClientProtocolConnection in a Service.
 */
public class ClientProtocolConnectionService extends AbstractIdleService 
        implements Reference<ClientProtocolConnection>, ClientExecutor {

    public static ClientProtocolConnectionService newInstance(
            Factory<ClientProtocolConnection> clientFactory) {
        return new ClientProtocolConnectionService(clientFactory);
    }
    
    protected final Factories.SynchronizedLazyHolder<ClientProtocolConnection> client;
    
    protected ClientProtocolConnectionService(
            Factory<ClientProtocolConnection> clientFactory) {
        this.client = Factories.synchronizedLazyFrom(clientFactory);
    }
    
    @Override
    protected void startUp() throws Exception {
        this.client.get().connect();
    }

    @Override
    protected void shutDown() throws Exception {
        if (this.client.has()) {
            ClientProtocolConnection client = this.client.get();
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
    public ClientProtocolConnection get() {
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
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Override
    public RequestFuture submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        return get().submit(request, promise);
    }
}
