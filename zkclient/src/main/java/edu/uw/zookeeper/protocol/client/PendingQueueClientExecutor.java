package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

import net.engio.mbassy.common.IConcurrentSet;

import com.google.common.base.Objects;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public abstract class PendingQueueClientExecutor<
    I extends Operation.Request, 
    V extends Operation.ProtocolResponse<?>,
    T extends PendingQueueClientExecutor.RequestTask<I, V>,
    C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>>
    extends AbstractConnectionClientExecutor<I,V,T,C,PendingQueueClientExecutor.PendingTask<V>> {

    protected final ConcurrentLinkedQueue<T> mailbox;
    protected final ConcurrentLinkedQueue<PendingTask<V>> pending;
    
    protected PendingQueueClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            IConcurrentSet<SessionListener> listeners) {
        super(session, connection, timeOut, executor, listeners);
        
        this.pending = Queues.newConcurrentLinkedQueue();
        this.mailbox = Queues.newConcurrentLinkedQueue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleConnectionRead(Operation.Response message) {
        super.handleConnectionRead(message);
        
        if (state() != State.TERMINATED) {
            if (message instanceof Operation.ProtocolResponse<?>) {
                int xid = ((Operation.ProtocolResponse<?>) message).xid();
                if (! ((xid == OpCodeXid.PING.xid()) || (xid == OpCodeXid.NOTIFICATION.xid()))) {
                    PendingTask<V> next = pending.peek();
                    if ((next != null) && (next.xid() == xid)) {
                        pending.remove(next);
                        next.set((V) message);
                    } else {
                        // This could happen if someone submitted a message without
                        // going through us
                        // or, it could be a bug
                        logger.warn("{}.xid != {}.xid ({})", next, message, this);
                    }
                }
            }
        }
    }
    
    @Override
    public void onSuccess(PendingTask<V> result) {
        // mark pings as done on write because ZooKeeper doesn't care about their ordering
        if (result.xid() == OpCodeXid.PING.xid()) {
            result.set(null);
        }
    }

    @Override
    protected ConcurrentLinkedQueue<T> mailbox() {
        return mailbox;
    }

    @Override
    protected void doStop() {
        Throwable failure = this.failure.get();
        PendingTask<V> task;
        while ((task = pending.poll()) != null) {
            if (failure == null) {
                task.cancel(true);
            } else {
                task.setException(failure);
            }
        }
        
        super.doStop();
    }

    protected PendingTask<V> write(Message.ClientRequest<?> message, Promise<V> promise) {
        PendingTask<V> task = PendingTask.of(message.xid(), this, promise);
        if (task.xid() != OpCodeXid.PING.xid()) {
            // task needs to be in the queue before calling write
            pending.add(task);
        }
        try {
            ListenableFuture<? extends Message.ClientRequest<?>> writeFuture = connection.write(message);
            Futures.addCallback(writeFuture, task, executor());
        } catch (Throwable t) {
            task.onFailure(t);
        }
        return task;
    }
    
    public static class RequestTask<I extends Operation.Request, V extends Operation.ProtocolResponse<?>> extends PromiseTask<I,V> {

        public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> RequestTask<I,V> of(I task, Promise<V> promise) {
            return new RequestTask<I,V>(task, promise);
        }
        
        public RequestTask(I task, Promise<V> promise) {
            super(task, promise);
        }
        
        public Promise<V> promise() {
            return delegate();
        }
    }
    
    public static class PendingTask<V extends Operation.ProtocolResponse<?>>
        extends PromiseTask<FutureCallback<? super PendingTask<V>>, V>
        implements Operation.RequestId, FutureCallback<Message.ClientRequest<?>> {
        
        public static <V extends Operation.ProtocolResponse<?>> PendingTask<V> of(
                int xid,
                FutureCallback<? super PendingTask<V>> callback,
                Promise<V> promise) {
            return new PendingTask<V>(xid, callback, promise);
        }

        protected final int xid;
        
        public PendingTask(
                int xid,
                FutureCallback<? super PendingTask<V>> callback,
                Promise<V> promise) {
            super(callback, promise);
            this.xid = xid;
        }
        
        @Override
        public int xid() {
            return xid;
        }

        @Override
        public boolean setException(Throwable t) {
            boolean doSet = super.setException(t);
            if (doSet) {
                task.onFailure(t);
            }
            return doSet;
        }

        @Override
        public void onSuccess(Message.ClientRequest<?> result) {
            task.onSuccess(this);
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        protected Objects.ToStringHelper toString(Objects.ToStringHelper toString) {
            return super.toString(toString.add("xid", xid));
        }
    } 
}
