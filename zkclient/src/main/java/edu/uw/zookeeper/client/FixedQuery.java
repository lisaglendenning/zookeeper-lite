package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.concurrent.Callable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public final class FixedQuery<O extends Operation.ProtocolResponse<?>> extends AbstractPair<ImmutableList<? extends Records.Request>, ClientExecutor<? super Records.Request,O,?>> implements Callable<List<ListenableFuture<O>>> {

    public static <O extends Operation.ProtocolResponse<?>> FixedQuery<O> forRequests(ClientExecutor<? super Records.Request,O,?> client, Records.Request...requests) {
        return forIterable(client, ImmutableList.copyOf(requests));
    }
    
    public static <O extends Operation.ProtocolResponse<?>> FixedQuery<O> forIterable(ClientExecutor<? super Records.Request,O,?> client, Iterable<? extends Records.Request> requests) {
        return new FixedQuery<O>(ImmutableList.copyOf(requests), checkNotNull(client));
    }
    
    protected FixedQuery(ImmutableList<? extends Records.Request> requests, ClientExecutor<? super Records.Request,O,?> client) {
        super(requests, client);
    }
    
    public ImmutableList<? extends Records.Request> requests() {
        return first;
    }
    
    public ClientExecutor<? super Records.Request,O,?> client() {
        return second;
    }
    
    @Override
    public List<ListenableFuture<O>> call() {
        List<ListenableFuture<O>> responses = Lists.newArrayListWithCapacity(requests().size());
        for (Records.Request request: requests()) {
            responses.add(client().submit(request));
        }
        return responses;
    }
}
