package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import com.google.common.collect.Queues;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public abstract class PendingQueueClientExecutor<I extends Operation.Request, 
    V extends PromiseTask<I, ? extends Message.ServerResponse<?>>,
    C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>>
    extends AbstractConnectionClientExecutor<I,V,C,PendingQueueClientExecutor.PendingTask> {

    protected final ConcurrentLinkedQueue<V> mailbox;
    protected final ConcurrentLinkedQueue<PendingTask> pending;
    
    protected PendingQueueClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super(session, connection, timeOut, executor);
        
        this.pending = Queues.newConcurrentLinkedQueue();
        this.mailbox = Queues.newConcurrentLinkedQueue();
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) {
        super.handleResponse(message);
        if (state() != State.TERMINATED) {
            int xid = message.xid();
            if (! ((xid == OpCodeXid.PING.xid()) || (xid == OpCodeXid.NOTIFICATION.xid()))) {
                PendingTask next = pending.peek();
                if ((next != null) && (next.xid() == xid)) {
                    next.set(message);
                    pending.remove(next);
                } else {
                    // This could happen if someone submitted a message without
                    // going through us
                    // or, it could be a bug
                    logger.warn("{}.xid != {}.xid ({})", next, message, this);
                }
            }
        }
    }
    
    @Override
    public void onSuccess(PendingTask result) {
        // mark pings as done on write because ZooKeeper doesn't care about their ordering
        if (result.xid() == OpCodeXid.PING.xid()) {
            result.set(null);
        }
    }

    @Override
    protected ConcurrentLinkedQueue<V> mailbox() {
        return mailbox;
    }

    @Override
    protected void doStop() {
        Throwable failure = this.failure.get();
        PendingTask task;
        while ((task = pending.poll()) != null) {
            if (failure == null) {
                task.cancel(true);
            } else {
                task.setException(failure);
            }
        }
        
        super.doStop();
    }
    
    protected static class PendingTask
        extends PromiseTask<FutureCallback<? super PendingTask>, Message.ServerResponse<?>>
        implements Operation.RequestId, FutureCallback<Message.ClientRequest<?>> {

        protected final int xid;
        
        public PendingTask(
                int xid,
                FutureCallback<? super PendingTask> callback,
                Promise<Message.ServerResponse<?>> promise) {
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
    } 
}
