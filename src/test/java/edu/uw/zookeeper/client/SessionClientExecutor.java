package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

public class SessionClientExecutor<T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> implements ClientExecutor<Records.Request, T, V> {

    public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> SessionClientExecutor<T,V> create(
            long sessionId,
            ClientExecutor<SessionOperation.Request<Records.Request>, T, V> delegate) {
        return new SessionClientExecutor<T,V>(sessionId, delegate);
    }
    
    protected final SessionClientProcessor sessionProcessor;
    protected final ClientExecutor<SessionOperation.Request<Records.Request>, T, V> delegate;
    
    public SessionClientExecutor(
            long sessionId,
            ClientExecutor<SessionOperation.Request<Records.Request>, T, V> delegate) {
        this.sessionProcessor = SessionClientProcessor.create(sessionId);
        this.delegate = delegate;
    }
    
    @Override
    public ListenableFuture<Pair<T, V>> submit(Records.Request request) {
        return delegate.submit(sessionProcessor.apply(request));
    }

    @Override
    public ListenableFuture<Pair<T, V>> submit(Records.Request request,
            Promise<Pair<T, V>> promise) {
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