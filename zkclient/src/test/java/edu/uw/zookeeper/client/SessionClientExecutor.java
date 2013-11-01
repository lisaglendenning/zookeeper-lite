package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

public class SessionClientExecutor<V extends Operation.ProtocolResponse<?>, T extends SessionListener> implements ClientExecutor<Records.Request, V, T> {

    public static <V extends Operation.ProtocolResponse<?>, T extends SessionListener> SessionClientExecutor<V,T> create(
            long sessionId,
            ClientExecutor<SessionOperation.Request<?>, V, T> delegate) {
        return new SessionClientExecutor<V,T>(sessionId, delegate);
    }
    
    protected final SessionClientProcessor sessionProcessor;
    protected final ClientExecutor<SessionOperation.Request<?>, V, T> delegate;
    
    public SessionClientExecutor(
            long sessionId,
            ClientExecutor<SessionOperation.Request<?>, V, T> delegate) {
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
    public void subscribe(T handler) {
        delegate.subscribe(handler);
    }

    @Override
    public boolean unsubscribe(T handler) {
        return delegate.unsubscribe(handler);
    }
}
