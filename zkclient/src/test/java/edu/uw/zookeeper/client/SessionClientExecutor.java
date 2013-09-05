package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

public class SessionClientExecutor<V extends Operation.ProtocolResponse<?>> implements ClientExecutor<Records.Request, V> {

    public static <T extends Operation.ProtocolRequest<?>, V extends Operation.ProtocolResponse<?>> SessionClientExecutor<V> create(
            long sessionId,
            ClientExecutor<SessionOperation.Request<?>, V> delegate) {
        return new SessionClientExecutor<V>(sessionId, delegate);
    }
    
    protected final SessionClientProcessor sessionProcessor;
    protected final ClientExecutor<SessionOperation.Request<?>, V> delegate;
    
    public SessionClientExecutor(
            long sessionId,
            ClientExecutor<SessionOperation.Request<?>, V> delegate) {
        this.sessionProcessor = SessionClientProcessor.create(sessionId);
        this.delegate = delegate;
    }
    
    @Override
    public ListenableFuture<V> submit(Records.Request request) {
        return delegate.submit(sessionProcessor.apply(request));
    }

    @Override
    public ListenableFuture<V> submit(Records.Request request,
            Promise<V> promise) {
        return delegate.submit(sessionProcessor.apply(request), promise);
    }
    
    @Override
    public void register(Object handler) {
        delegate.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
    }
    
}