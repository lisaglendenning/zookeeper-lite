package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Operation;

public class SubmitGenerator<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends AbstractPair<Generator<? extends I>, ClientExecutor<? super I, O, ?>> implements Generator<Pair<I, ListenableFuture<O>>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmitGenerator<I,O> create(
            Generator<? extends I> operations,
            ClientExecutor<? super I, O, ?> client) {
        return new SubmitGenerator<I,O>(operations, client);
    }
    
    protected SubmitGenerator(
            Generator<? extends I> operations,
            ClientExecutor<? super I, O, ?> client) {
        super(operations, client);
    }
    
    public Generator<? extends I> getOperations() {
        return first;
    }
    
    public ClientExecutor<? super I,O, ?> getClient() {
        return second;
    }
    
    @Override
    public Pair<I, ListenableFuture<O>> next() {
        final I request = getOperations().next();
        return Pair.create(request, getClient().submit(request));
    }
}
