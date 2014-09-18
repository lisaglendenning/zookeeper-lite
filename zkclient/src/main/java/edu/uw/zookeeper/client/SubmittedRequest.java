package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.ValueFuture;
import edu.uw.zookeeper.protocol.Operation;

public final class SubmittedRequest<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends ValueFuture<I,O,ListenableFuture<O>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequest<I,O> submit(
            ClientExecutor<? super I,O,?> client, I request) {
        return new SubmittedRequest<I,O>(request, client.submit(request));
    }
    
    protected SubmittedRequest(I value, ListenableFuture<O> future) {
        super(value, future);
    }
}
