package edu.uw.zookeeper.client;

import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.protocol.Operation;

public final class SubmittedRequests<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends ForwardingListenableFuture<List<O>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequests<I,O> submitRequests(ClientExecutor<? super I,O,?> client, I... requests) {
        return submit(client, ImmutableList.copyOf(requests));
    }
    
    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequests<I,O> submit(ClientExecutor<? super I,O,?> client, Iterable<? extends I> requests) {
        final ImmutableList.Builder<ListenableFuture<O>> futures = ImmutableList.builder();
        for (I request: requests) {
            futures.add(client.submit(request));
        }
        return new SubmittedRequests<I,O>(
                ImmutableList.copyOf(requests),
                Futures.allAsList(futures.build()));
    }
    
    private final ImmutableList<I> requests;
    private final ListenableFuture<List<O>> future;
    
    protected SubmittedRequests(ImmutableList<I> requests, ListenableFuture<List<O>> future) {
        this.requests = requests;
        this.future = future;
    }
    
    public ImmutableList<I> requests() {
        return requests;
    }
    
    @Override
    protected ListenableFuture<List<O>> delegate() {
        return future;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("requests", requests).add("future", LoggingFutureListener.toString(future)).toString();
    }
}
