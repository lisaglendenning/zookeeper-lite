package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;
import edu.uw.zookeeper.protocol.OpAction;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Singleton;

/**
 * Wraps a lazily-instantiated ClientProtocolExecutor in a Service.
 */
public class ClientProtocolConnectionService extends AbstractIdleService implements Singleton<ClientProtocolConnection> {

    public static ClientProtocolConnectionService newInstance(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<ClientProtocolConnection> clientFactory) {
        return new ClientProtocolConnectionService(processor, clientFactory);
    }
    
    protected final Processor<Operation.Request, Operation.SessionRequest> processor;
    protected final Singleton<ClientProtocolConnection> client;
    
    protected ClientProtocolConnectionService(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<ClientProtocolConnection> clientFactory) {
        this.processor = processor;
        this.client = Factories.lazyFrom(clientFactory);
    }
    
    @Override
    protected void startUp() throws Exception {
        ClientProtocolConnection client = get();
        client.connect().get();
    }

    @Override
    protected void shutDown() throws Exception {
        ClientProtocolConnection client = get();
        Operation.SessionRequest message = processor.apply(OpAction.Request.create(OpCode.CLOSE_SESSION));
        client.submit(message).get();
    }

    @Override
    public synchronized ClientProtocolConnection get() {
        return client.get();
    }
}
