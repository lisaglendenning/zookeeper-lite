package edu.uw.zookeeper.protocol.server;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

public class ServerTaskExecutor {
    
    public static ServerTaskExecutor newInstance(
            TaskExecutor<FourLetterRequest, FourLetterResponse> anonymousExecutor,
            TaskExecutor<Pair<ConnectMessage.Request, Publisher>, ConnectMessage.Response> connectExecutor,
            TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> sessionExecutor) {
        return new ServerTaskExecutor(anonymousExecutor, connectExecutor, sessionExecutor);
    }

    public static class ProcessorExecutor<I,O> implements TaskExecutor<I,O> {

        public static <I,O> ProcessorExecutor<I,O> of(
                Processor<? super I, ? extends O> delegate) {
            return new ProcessorExecutor<I,O>(delegate);
        }
        
        protected final Processor<? super I, ? extends O> delegate;
        
        public ProcessorExecutor(Processor<? super I, ? extends O> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public ListenableFuture<O> submit(I request) {
            try {
                return Futures.immediateFuture((O) delegate.apply(request));
            } catch (Exception e) {
                return Futures.immediateFailedCheckedFuture(e);
            }
        }
    }
    
    protected final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor;
    protected final TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor;
    protected final TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor;

    public ServerTaskExecutor(            
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        this.anonymousExecutor = anonymousExecutor;
        this.connectExecutor = connectExecutor;
        this.sessionExecutor = sessionExecutor;
    }
    
    public TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> getAnonymousExecutor() {
        return anonymousExecutor;
    }
    
    public TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> getConnectExecutor() {
        return connectExecutor;
    }
    
    public TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> getSessionExecutor() {
        return sessionExecutor;
    }
}
