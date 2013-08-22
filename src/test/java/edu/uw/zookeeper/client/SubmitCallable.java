package edu.uw.zookeeper.client;

import java.util.concurrent.Callable;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Operation;

public class SubmitCallable<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> implements Callable<Pair<I, ListenableFuture<O>>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmitCallable<I,O> create(
            Generator<I> operations,
            ClientExecutor<? super I, O> client) {
        return new SubmitCallable<I,O>(operations, client);
    }
    
    protected final Generator<I> operations;
    protected final ClientExecutor<? super I, O> client;
    
    public SubmitCallable(
            Generator<I> operations,
            ClientExecutor<? super I, O> client) {
        this.operations = operations;
        this.client = client;
    }
    
    @Override
    public Pair<I, ListenableFuture<O>> call() throws Exception {
        I request = operations.next();
        return Pair.create(request, client.submit(request));
    }
}
