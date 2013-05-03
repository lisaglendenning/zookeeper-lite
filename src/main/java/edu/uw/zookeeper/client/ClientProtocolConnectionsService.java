package edu.uw.zookeeper.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Processor;

/**
 * Wraps a SessionClient Factory in a Service.
 */
public class ClientProtocolConnectionsService extends AbstractIdleService implements Iterable<SessionClient>, Factory<SessionClient> {

    public static ClientProtocolConnectionsService newInstance(
            Factory<SessionClient> clientFactory) {
        return newInstance(AssignXidProcessor.newInstance(), clientFactory);
    }
    
    public static ClientProtocolConnectionsService newInstance(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<SessionClient> clientFactory) {
        return new ClientProtocolConnectionsService(processor, clientFactory);
    }
    
    protected final Factory<SessionClient> clientFactory;
    protected final List<SessionClient> clients;
    
    protected ClientProtocolConnectionsService(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<SessionClient> clientFactory) {
        this.clientFactory = clientFactory;
        this.clients = Collections.synchronizedList(Lists.<SessionClient>newArrayList());
    }
    
    @Override
    protected void startUp() throws Exception {
        for (SessionClient client: clients) {
            client.connect();   
        }
    }

    @Override
    protected void shutDown() throws Exception {
        for (SessionClient client: clients) {
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
    public SessionClient get() {
        State state = state();
        switch (state) {
        case STOPPING:
        case TERMINATED:
            throw new IllegalStateException(state.toString());
        default:
            break;
        }
        SessionClient client = clientFactory.get();
        clients.add(client);
        return client;
    }

    @Override
    public Iterator<SessionClient> iterator() {
        return clients.iterator();
    }
}
