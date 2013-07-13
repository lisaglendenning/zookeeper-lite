package edu.uw.zookeeper.protocol.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Factory;

/**
 * Wraps a ClientConnectionExecutor Factory in a Service.
 */
public class ClientConnectionExecutorsService<T extends Connection<? super Operation.Request>> extends AbstractIdleService implements Iterable<ClientConnectionExecutor<T>>, Factory<ClientConnectionExecutor<T>> {

    public static <T extends Connection<? super Operation.Request>> ClientConnectionExecutorsService<T> newInstance(
            Factory<ClientConnectionExecutor<T>> clientFactory) {
        return new ClientConnectionExecutorsService<T>(clientFactory);
    }
    
    protected final Factory<ClientConnectionExecutor<T>> clientFactory;
    protected final List<ClientConnectionExecutor<T>> clients;
    
    protected ClientConnectionExecutorsService(
            Factory<ClientConnectionExecutor<T>> clientFactory) {
        this.clientFactory = clientFactory;
        this.clients = Collections.synchronizedList(Lists.<ClientConnectionExecutor<T>>newLinkedList());
    }
    
    public Factory<ClientConnectionExecutor<T>> factory() {
        return clientFactory;
    }
    
    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        for (ClientConnectionExecutor<T> client: clients) {
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

    @Override
    public ClientConnectionExecutor<T> get() {
        State state = state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        ClientConnectionExecutor<T> client = clientFactory.get();
        // TODO: if shutdown gets called here, we'll miss this client...
        clients.add(client);
        return client;
    }

    @Override
    public Iterator<ClientConnectionExecutor<T>> iterator() {
        return clients.iterator();
    }
}
