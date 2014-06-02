package edu.uw.zookeeper.client;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public class IteratingClient extends PromiseTask<Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>>, Void> implements Runnable {
    
    public static IteratingClient create(
            Executor executor,
            Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> operations,
            Promise<Void> promise) {
        return new IteratingClient(executor, operations, promise);
    }
    
    protected final Executor executor;
    protected final Set<PendingOperation> pending;
    
    protected IteratingClient(
            Executor executor,
            Iterator<? extends Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>>> callable,
            Promise<Void> promise) {
        super(callable, promise);
        this.executor = executor;
        this.pending = Sets.newConcurrentHashSet();
        
        addListener(this, executor);
    }
    
    @Override
    public synchronized void run() {
        if (!isDone()) {
            if (task().hasNext()) {
                Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation;
                try {
                    operation = task().next();
                } catch (Exception e) {
                    setException(e);
                    return;
                }
                new PendingOperation(operation);
                executor.execute(this);
            } else if (pending.isEmpty()) {
                set(null);
            }
        } else {
            Iterator<PendingOperation> itr = Iterators.consumingIterator(pending.iterator());
            while (itr.hasNext()) {
                itr.next().cancel(true);
            }
        }
    }
    
    protected class PendingOperation extends ForwardingListenableFuture<Operation.ProtocolResponse<?>> implements Runnable {
        private final Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation;

        public PendingOperation(Pair<? extends Records.Request, ? extends ListenableFuture<? extends Operation.ProtocolResponse<?>>> operation) {
            this.operation = operation;
            pending.add(this);
            addListener(this, executor);
        }
        
        @Override
        public void run() {
            synchronized (IteratingClient.this) {
                if (isDone()) {
                    if (pending.remove(this)) {
                        if (!isCancelled()) {
                            try { 
                                get();
                            } catch (InterruptedException e) {
                                throw new AssertionError(e);
                            } catch (ExecutionException e) {
                                setException(e.getCause());
                            }
                        }
                    }
                    IteratingClient.this.run();
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        protected ListenableFuture<Operation.ProtocolResponse<?>> delegate() {
            return (ListenableFuture<Operation.ProtocolResponse<?>>) (ListenableFuture<?>) operation.second();
        }
    }
}