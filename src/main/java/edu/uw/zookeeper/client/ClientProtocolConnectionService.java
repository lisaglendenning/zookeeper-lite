package edu.uw.zookeeper.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated ClientProtocolConnection in a Service.
 */
public class ClientProtocolConnectionService extends AbstractIdleService implements Reference<ClientProtocolConnection> {

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
    protected void startUp() throws IOException {
        this.client.get().connect();
    }

    @Override
    protected void shutDown() throws InterruptedException, ExecutionException {
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
}
