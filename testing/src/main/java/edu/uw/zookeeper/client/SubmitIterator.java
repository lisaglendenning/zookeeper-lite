package edu.uw.zookeeper.client;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Operation;

public class SubmitIterator<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> extends AbstractIterator<Pair<I, ListenableFuture<O>>> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> SubmitIterator<I,O> create(
            Iterator<I> requests,
            ClientExecutor<? super I, O, ?> client) {
        return new SubmitIterator<I,O>(requests, client);
    }
    
    protected final Iterator<I> requests;
    protected final ClientExecutor<? super I, O, ?> client;
    
    public SubmitIterator(
            Iterator<I> requests,
            ClientExecutor<? super I, O, ?> client) {
        this.requests = requests;
        this.client = client;
    }

    @Override
    protected Pair<I, ListenableFuture<O>> computeNext() {
        if (requests.hasNext()) {
            I request = requests.next();
            return Pair.create(request, client.submit(request));
        } else {
            return endOfData();
        }
    }
}
