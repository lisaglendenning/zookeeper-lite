package edu.uw.zookeeper.client;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.protocol.Operation;

public final class SubmittedRequest<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends ForwardingListenableFuture<O> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequest<I,O> submit(
            ClientExecutor<? super I,O,?> client, I request) {
        return new SubmittedRequest<I,O>(request, client.submit(request));
    }
    
    private final I request;
    private final ListenableFuture<O> future;
    
    protected SubmittedRequest(I request, ListenableFuture<O> future) {
        this.request = request;
        this.future = future;
    }
    
    public I request() {
        return request;
    }
    
    @Override
    protected ListenableFuture<O> delegate() {
        return future;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("request", request).add("future", LoggingFutureListener.toString(future)).toString();
    }
}