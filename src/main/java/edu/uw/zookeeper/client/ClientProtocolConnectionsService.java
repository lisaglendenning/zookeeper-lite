package edu.uw.zookeeper.client;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.Factory;

/**
 * Wraps a ClientProtocolConnection Factory in a Service.
 */
public class ClientProtocolConnectionsService extends AbstractIdleService implements Iterable<ClientProtocolConnection>, Factory<ClientProtocolConnection> {

    public static ClientProtocolConnectionsService newInstance(
            Factory<ClientProtocolConnection> clientFactory) {
        return new ClientProtocolConnectionsService(clientFactory);
    }
    
    protected final Factory<ClientProtocolConnection> clientFactory;
    protected final List<ClientProtocolConnection> clients;
    
    protected ClientProtocolConnectionsService(
            Factory<ClientProtocolConnection> clientFactory) {
        this.clientFactory = clientFactory;
        this.clients = Collections.synchronizedList(Lists.<ClientProtocolConnection>newLinkedList());
    }
    
    @Override
    protected void startUp() throws IOException {
        for (ClientProtocolConnection client: clients) {
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
    protected void shutDown() throws InterruptedException, ExecutionException {
        for (ClientProtocolConnection client: clients) {
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
        ClientProtocolConnection client = clientFactory.get();
        // TODO: if shutdown gets called here, we'll miss this client...
        clients.add(client);
        try {
            client.connect();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return client;
    }

    @Override
    public Iterator<ClientProtocolConnection> iterator() {
        return clients.iterator();
    }
}
