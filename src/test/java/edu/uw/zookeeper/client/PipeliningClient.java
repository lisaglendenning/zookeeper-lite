package edu.uw.zookeeper.client;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Operation;

public class PipeliningClient<I extends Operation.Request, V extends Operation.ProtocolResponse<?>> implements Generator<List<Pair<I, ListenableFuture<V>>>> {

    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> PipeliningClient<I,V> create(
            int pipelineLength,
            ClientExecutor<? super I, V> client,
            Generator<I> operations) {
        return new PipeliningClient<I,V>(pipelineLength, client, operations);
    }
    
    protected final int pipelineLength;
    protected final ClientExecutor<? super I, V> client;
    protected final Generator<I> operations;
    
    public PipeliningClient(
            int pipelineLength,
            ClientExecutor<? super I, V> client,
            Generator<I> operations) {
        this.pipelineLength = pipelineLength;
        this.client = client;
        this.operations = operations;
    }
    
    @Override
    public List<Pair<I, ListenableFuture<V>>> next() {
        List<Pair<I, ListenableFuture<V>>> futures = Lists.newArrayListWithCapacity(pipelineLength);
        for (int i=0; i<pipelineLength; ++i) {
            I request = operations.next();
            ListenableFuture<V> future = client.submit(request);
            futures.add(Pair.create(request, future));
        }
        return futures;
    }
}
