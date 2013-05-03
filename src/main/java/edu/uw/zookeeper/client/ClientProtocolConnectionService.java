package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Reference;

/**
 * Wraps a lazily-instantiated SessionClient in a Service.
 */
public class ClientProtocolConnectionService extends AbstractIdleService implements Reference<SessionClient> {

    public static ClientProtocolConnectionService newInstance(
            Factory<SessionClient> clientFactory) {
        return new ClientProtocolConnectionService(clientFactory);
    }
    
    protected final Factories.LazyHolder<SessionClient> client;
    
    protected ClientProtocolConnectionService(
            Factory<SessionClient> clientFactory) {
        this.client = Factories.lazyFrom(clientFactory);
    }
    
    @Override
    protected void startUp() {
        get().connect();
    }

    @Override
    protected void shutDown() {
        if (client.has()) {
            SessionClient client = get();
            switch (client.get().state()) {
            case CONNECTING:
            case CONNECTED:
                client.disconnect();
                break;
            default:
                break;
            }
        }
    }

    @Override
    public synchronized SessionClient get() {
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
