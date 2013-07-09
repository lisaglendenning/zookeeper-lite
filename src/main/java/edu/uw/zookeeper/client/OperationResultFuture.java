package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Pair;

public class OperationResultFuture 
        extends ForwardingListenableFuture<Pair<Operation.SessionRequest, Operation.SessionResponse>>
        implements FutureCallback<Operation.SessionResponse> {
    
    public static OperationResultFuture of(Operation.SessionRequest request, ListenableFuture<Operation.SessionResponse> future) {
        return new OperationResultFuture(request, future);
    }

    protected final SettableFuture<Pair<Operation.SessionRequest, Operation.SessionResponse>> delegate;
    protected final Operation.SessionRequest request;
    protected final ListenableFuture<Operation.SessionResponse> future;
    
    protected OperationResultFuture(Operation.SessionRequest request, ListenableFuture<Operation.SessionResponse> future) {
        super();
        this.request = request;
        this.future = future;
        this.delegate = SettableFuture.create();
        Futures.addCallback(future, this);
    }
    
    public Operation.SessionRequest request() {
        return request;
    }
    
    protected ListenableFuture<Pair<Operation.SessionRequest, Operation.SessionResponse>> delegate() {
        return delegate;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (future.cancel(mayInterruptIfRunning)) {
            super.cancel(mayInterruptIfRunning);
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public void onSuccess(Operation.SessionResponse result) {
        delegate.set(Pair.create(request(), result));
    }

    @Override
    public void onFailure(Throwable t) {
        delegate.setException(t);
    }
}
