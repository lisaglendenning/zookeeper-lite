package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.protocol.OpSessionResult;
import edu.uw.zookeeper.protocol.Operation;

public class OperationResultFuture 
        extends ForwardingListenableFuture<Operation.SessionResult>
        implements FutureCallback<Operation.SessionReply> {
    
    public static OperationResultFuture of(Operation.SessionRequest request, ListenableFuture<Operation.SessionReply> future) {
        return new OperationResultFuture(request, future);
    }

    protected final SettableFuture<Operation.SessionResult> delegate;
    protected final Operation.SessionRequest request;
    protected final ListenableFuture<Operation.SessionReply> future;
    
    protected OperationResultFuture(Operation.SessionRequest request, ListenableFuture<Operation.SessionReply> future) {
        super();
        this.request = request;
        this.future = future;
        this.delegate = SettableFuture.create();
        Futures.addCallback(future, this);
    }
    
    public Operation.SessionRequest request() {
        return request;
    }
    
    protected ListenableFuture<Operation.SessionResult> delegate() {
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
    public void onSuccess(Operation.SessionReply result) {
        delegate.set(OpSessionResult.of(request(), result));
    }

    @Override
    public void onFailure(Throwable t) {
        delegate.setException(t);
    }
}
