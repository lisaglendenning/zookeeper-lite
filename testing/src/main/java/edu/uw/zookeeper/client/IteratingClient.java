package edu.uw.zookeeper.client;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public final class IteratingClient extends PromiseTask<Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Void> implements Runnable {
    
    public static IteratingClient create(
            Executor executor,
            Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> operations,
            Promise<Void> promise) {
        return new IteratingClient(executor, operations, promise);
    }
    
    private final Executor executor;
    private final AtomicInteger pending;
    
    protected IteratingClient(
            Executor executor,
            Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> operations,
            Promise<Void> promise) {
        super(operations, promise);
        this.executor = executor;
        this.pending = new AtomicInteger(0);
    }
    
    @Override
    public synchronized void run() {
        if (!isDone()) {
            // note that iterator calls are potentially blocking
            if (task().hasNext()) {
                Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation;
                try {
                    operation = task().next();
                } catch (Exception e) {
                    setException(e);
                    return;
                }
                pending.incrementAndGet();
                new PendingOperation(operation.second());
                executor.execute(this);
            } else if (pending.get() == 0) {
                set(null);
            }
        }
    }
    
    protected final class PendingOperation implements Runnable {
        
        private final ListenableFuture<? extends Operation.ProtocolResponse<?>> future;

        public PendingOperation(ListenableFuture<? extends Operation.ProtocolResponse<?>> future) {
            this.future = future;
            future.addListener(this, executor);
        }
        
        @Override
        public void run() {
            if (future.isDone()) {
                try {
                    if (!future.isCancelled()) {
                        try { 
                            future.get();
                        } catch (InterruptedException e) {
                            throw Throwables.propagate(e);
                        } catch (ExecutionException e) {
                            setException(e);
                        }
                    }
                } finally {
                    if (pending.decrementAndGet() == 0) {
                        executor.execute(IteratingClient.this);
                    }
                }
            }
        }
    }
}