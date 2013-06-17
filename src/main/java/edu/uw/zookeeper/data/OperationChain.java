package edu.uw.zookeeper.data;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;

public class OperationChain extends PromiseTask<Function<Operation.SessionResult, Operation.Request>, List<Operation.SessionResult>> implements FutureCallback<Operation.SessionResult> {

    public static OperationChain of(
            Function<Operation.SessionResult, Operation.Request> callback,
            ClientExecutor client,
            Executor executor) {
        Promise<List<Operation.SessionResult>> promise = newPromise();
        return of(callback, client, executor, promise);
    }
    
    public static OperationChain of(
            Function<Operation.SessionResult, Operation.Request> callback,
            ClientExecutor client, 
            Executor executor,
            Promise<List<Operation.SessionResult>> promise) {
        return new OperationChain(callback, client, executor, promise);
    }
    
    protected final BlockingQueue<Operation.SessionResult> results;
    protected final ClientExecutor client;
    protected final Executor executor;
    protected volatile Pair<Operation.Request, ListenableFuture<Operation.SessionResult>> pending;
    
    protected OperationChain(
            Function<Operation.SessionResult, Operation.Request> callback,
            ClientExecutor client, 
            Executor executor,
            Promise<List<Operation.SessionResult>> promise) {
        super(callback, promise);
        this.client = client;
        this.executor = executor;
        this.results = new LinkedBlockingQueue<Operation.SessionResult>();
        this.pending = null;
        
        onSuccess(null);
    }
    
    public Pair<Operation.Request, ListenableFuture<Operation.SessionResult>> pending() {
        return pending;
    }
    
    public BlockingQueue<Operation.SessionResult> results() {
        return results;
    }
    
    @Override
    public void onSuccess(Operation.SessionResult result) {
        if (result != null) {
            try {
                results().put(result);
            } catch (InterruptedException e) {
                onFailure(e);
            }
        }
        
        Operation.Request request = task().apply(result);
        if (request != null) {
            ListenableFuture<Operation.SessionResult> future = client.submit(request);
            this.pending = Pair.create(request, future);
            Futures.addCallback(future, this, executor);
        } else {
            // done
            this.pending = null;
            List<Operation.SessionResult> results = Lists.newLinkedList();
            this.results.drainTo(results);
            set(results);
        }
    }
    
    @Override
    public void onFailure(Throwable t) {
        if (pending != null) {
            Operation.Request failedRequest = pending.first();
            t = new ExecutionException("Error executing " + failedRequest.toString(), t);
        }
        this.pending = null;
        setException(t);
    }
}