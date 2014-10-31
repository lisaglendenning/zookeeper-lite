package edu.uw.zookeeper.client;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.ValueFuture;
import edu.uw.zookeeper.protocol.Operation;

public final class SubmittedRequests<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends ValueFuture<ImmutableList<I>,List<O>,ListenableFuture<List<O>>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequests<I,O> submitRequests(TaskExecutor<? super I,? extends O> client, I... requests) {
        return submit(client, ImmutableList.copyOf(requests));
    }
    
    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmittedRequests<I,O> submit(TaskExecutor<? super I,? extends O> client, Iterable<? extends I> requests) {
        final ImmutableList.Builder<ListenableFuture<? extends O>> futures = ImmutableList.builder();
        for (I request: requests) {
            futures.add(client.submit(request));
        }
        return new SubmittedRequests<I,O>(
                ImmutableList.copyOf(requests),
                Futures.allAsList(futures.build()));
    }
    
    protected SubmittedRequests(ImmutableList<I> value, ListenableFuture<List<O>> future) {
        super(value, future);
    }
}
