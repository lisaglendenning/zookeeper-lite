package edu.uw.zookeeper.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.Factory;

/**
 * Wraps a ClientProtocolExecutor Factory in a Service.
 */
public class ClientProtocolExecutorsService extends AbstractIdleService implements Iterable<ClientProtocolExecutor>, Factory<ClientProtocolExecutor> {

    public static ClientProtocolExecutorsService newInstance(
            Factory<ClientProtocolExecutor> clientFactory) {
        return new ClientProtocolExecutorsService(clientFactory);
    }
    
    protected final Factory<ClientProtocolExecutor> clientFactory;
    protected final List<ClientProtocolExecutor> clients;
    
    protected ClientProtocolExecutorsService(
            Factory<ClientProtocolExecutor> clientFactory) {
        this.clientFactory = clientFactory;
        this.clients = Collections.synchronizedList(Lists.<ClientProtocolExecutor>newLinkedList());
    }
    
    public Factory<ClientProtocolExecutor> factory() {
        return clientFactory;
    }
    
    @Override
    protected void startUp() throws Exception {
        for (ClientProtocolExecutor client: clients) {
            switch (client.state()) {
            case ANONYMOUS:
                client.connect();
                break;
            default:
                break;
            }  
        }
    }

    @Override
    protected void shutDown() throws Exception {
        for (ClientProtocolExecutor client: clients) {
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
        ClientProtocolExecutor client = clientFactory.get();
        // TODO: if shutdown gets called here, we'll miss this client...
        clients.add(client);
        client.connect();
        return client;
    }

    @Override
    public Iterator<ClientProtocolExecutor> iterator() {
        return clients.iterator();
    }
}
