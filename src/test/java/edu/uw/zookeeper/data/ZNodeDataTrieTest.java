package edu.uw.zookeeper.data;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.client.SessionClientProcessor;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {
    
    public static class Client<T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> implements ClientExecutor<Records.Request, T, V> {

        public static <T extends Operation.ProtocolRequest<Records.Request>, V extends Operation.ProtocolResponse<Records.Response>> Client<T,V> create(
                long sessionId,
                ClientExecutor<SessionOperation.Request<Records.Request>, T, V> delegate) {
            return new Client<T,V>(sessionId, delegate);
        }
        
        protected final SessionClientProcessor sessionProcessor;
        protected final ClientExecutor<SessionOperation.Request<Records.Request>, T, V> delegate;
        
        public Client(
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
    
    @Test
    public void test() throws InterruptedException, ExecutionException {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create();
        ZNodeViewCache<?, Records.Request, SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> cache = 
                ZNodeViewCache.newInstance(executor, Client.create(1, executor));
        
        
        ZNodeLabel.Path path = ZNodeLabel.Path.of("/foo");
        cache.submit(Operations.Requests.create().setPath(path).build()).get();
        cache.submit(Operations.Requests.exists().setPath(path).build()).get();
        cache.submit(Operations.Requests.delete().setPath(path).build()).get();
    }
}
