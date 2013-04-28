package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.protocol.OpAction;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.SessionReply;
import edu.uw.zookeeper.protocol.Operation.SessionRequest;
import edu.uw.zookeeper.protocol.client.ClientProtocolExecutor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.LazyHolder;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Singleton;

/**
 * Wraps a lazily-instantiated ClientProtocolExecutor in a Service.
 */
public class ClientProtocolExecutorService extends AbstractIdleService implements SessionRequestExecutor, Singleton<ClientProtocolExecutor> {

    public static ClientProtocolExecutorService newInstance(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<ClientProtocolExecutor> clientFactory) {
        return new ClientProtocolExecutorService(processor, clientFactory);
    }
    
    protected final Processor<Operation.Request, Operation.SessionRequest> processor;
    protected final LazyHolder<ClientProtocolExecutor> client;
    
    protected ClientProtocolExecutorService(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<ClientProtocolExecutor> clientFactory) {
        this.processor = processor;
        this.client = LazyHolder.newInstance(clientFactory);
    }
    
    @Override
    protected void startUp() throws Exception {
        ClientProtocolExecutor client = get();
        client.connect().get();
    }

    @Override
    protected void shutDown() throws Exception {
        ClientProtocolExecutor client = get();
        Operation.SessionRequest message = processor.apply(OpAction.Request.create(OpCode.CLOSE_SESSION));
        client.submit(message).get();
    }

    @Override
    public synchronized ClientProtocolExecutor get() {
        return client.get();
    }

    @Override
    public ListenableFuture<SessionReply> submit(SessionRequest request) {
        State state = state();
        switch (state) {
        case STARTING:
        case RUNNING:
            break;
        default:
            throw new IllegalStateException(state.toString());
        }
        return get().submit(request);
    }
}
