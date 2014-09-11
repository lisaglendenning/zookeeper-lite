package edu.uw.zookeeper.protocol.client;

import java.lang.ref.Reference;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actors;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public abstract class PendingQueueClientExecutor<
    I extends Operation.Request, 
    O extends Operation.ProtocolResponse<?>,
    T extends PendingQueueClientExecutor.RequestTask<I,O>,
    C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>,
    V extends PendingQueueClientExecutor.PendingTask>
    extends AbstractConnectionClientExecutor<I,O,T,C,V> {

    protected final Pending<O,V,C> pending;

    protected PendingQueueClientExecutor(
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        super(session, connection, timeOut, scheduler);
        this.pending = Pending.<O,V,C>create(connection, this, logger);
    }
    
    protected PendingQueueClientExecutor(
            Logger logger,
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            Listeners listeners,
            TimeOutServer<Operation.Response> timer,
            AtomicReference<Throwable> failure) {
        super(session, connection, listeners, timer, failure);
        this.pending = Pending.<O,V,C>create(connection, this, logger);
    }

    @Override
    public void handleConnectionRead(Operation.Response message) {
        super.handleConnectionRead(message);
        pending.handleConnectionRead(message);
    }
    
    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> event) {
        super.handleConnectionState(event);
        pending.handleConnectionState(event);
    }
    
    @Override
    protected void doStop() {
        pending.stop();
        super.doStop();
    }
    
    protected Executor executor() {
        return connection;
    }

    public static class PendingTask
        extends PromiseTask<Reference<? extends Message.ClientRequest<?>>, Message.ServerResponse<?>>
        implements Operation.RequestId, FutureCallback<Message.ClientRequest<?>> {
        
        public static PendingTask create(
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            return new PendingTask(task, delegate);
        }

        protected final int xid;
        
        public PendingTask(
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            super(task, delegate);
            this.xid = task().get().xid();
        }
        
        @Override
        public int xid() {
            return xid;
        }
        
        public Message.ClientRequest<?> getRequest() {
            return task().get();
        }
        
        @Override
        public void onSuccess(Message.ClientRequest<?> result) {
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }

        @Override
        protected MoreObjects.ToStringHelper toStringHelper(MoreObjects.ToStringHelper toString) {
            return super.toStringHelper(toString.add("xid", xid));
        }
    } 
    
    public static class PendingPromiseTask extends PendingTask {

        public static PendingPromiseTask create(
                Promise<Message.ServerResponse<?>> promise,
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            return new PendingPromiseTask(promise, task, delegate);
        }

        protected final Promise<Message.ServerResponse<?>> promise;

        public PendingPromiseTask(
                Promise<Message.ServerResponse<?>> promise,
                Reference<? extends Message.ClientRequest<?>> task,
                Promise<Message.ServerResponse<?>> delegate) {
            super(task, delegate);
            this.promise = promise;
        }

        public Promise<Message.ServerResponse<?>> getPromise() {
            return promise;
        }
    } 
    
    public static class Pending<
    O extends Operation.ProtocolResponse<?>,
    T extends PendingTask,
    C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> extends Actors.ExecutedPeekingQueuedActor<T> implements Connection.Listener<Operation.Response> {

        public static <O extends Operation.ProtocolResponse<?>,T extends PendingTask,C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>> Pending<O,T,C> create(
                C connection,
                FutureCallback<? super T> callback,
                Logger logger) {
            return new Pending<O,T,C>(connection, callback, Queues.<T>newConcurrentLinkedQueue(), logger);
        }

        protected final FutureCallback<? super T> callback;
        protected final C connection;
        
        protected Pending(
                C connection,
                FutureCallback<? super T> callback,
                Queue<T> mailbox,
                Logger logger) {
            super(connection, mailbox, logger);
            this.callback = callback;
            this.connection = connection;
        }

        @Override
        protected synchronized boolean doSend(T message) {
            // we use synchronized to preserve queue/send ordering
            // task needs to be in the queue before calling write
            if (! mailbox.offer(message)) {
                return false;
            }
            try {
                if (! message.isDone()) {
                    // mark pings as done on send because ZooKeeper doesn't care about their ordering
                    if (message.xid() == OpCodeXid.PING.xid()) {
                        message.set(null);
                    }
                    Message.ClientRequest<?> request = message.getRequest();
                    if (request != null) {
                        ListenableFuture<? extends Message.ClientRequest<?>> writeFuture = connection.write(request);
                        Futures.addCallback(writeFuture, message);
                    }
                }
            } catch (Throwable t) {
                message.onFailure(t);
            }
            if (state() == State.TERMINATED) {
                message.cancel(true);
                apply(message);
            }
            if (! message.isDone()) {
                message.addListener(this, executor);
            } else {
                run();
            }
            return true;
        }
        
        @Override
        public void handleConnectionRead(Operation.Response message) {
            if (message instanceof Message.ServerResponse<?>) {
                int xid = ((Message.ServerResponse<?>) message).xid();
                if (! ((xid == OpCodeXid.PING.xid()) || (xid == OpCodeXid.NOTIFICATION.xid()))) {
                    Iterator<T> tasks = mailbox.iterator();
                    T task = null;
                    while (tasks.hasNext()) {
                        T next = tasks.next();
                        if (next.xid() == xid) {
                            task = next;
                            break;
                        }
                    }
                    if (task != null) {
                        task.set((Message.ServerResponse<?>) message);
                    } else if (state() != State.TERMINATED) {
                        // This could happen if someone submitted a message without
                        // going through us
                        // or, it could be a bug
                        logger.warn("{} xid doesn't match {} ({})", message, mailbox.peek(), this);
                    }
                }
            }
        }

        @Override
        public void handleConnectionState(Automaton.Transition<Connection.State> state) {
            // TODO
        }

        public boolean isReady() {
            T next = mailbox.peek();
            return ((next != null) && next.isDone());
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).addValue(callback).toString();
        }
        
        @Override
        protected void doStop() {
            T task;
            while ((task = mailbox.peek()) != null) {
                task.cancel(true);
                apply(task);
            }
        }

        @Override
        protected synchronized boolean apply(T input) {
            if (input.isDone()) {
                if (mailbox.remove(input)) {
                    callback.onSuccess(input);
                    return true;
                }
            }
            return false;
        }
    }

    public abstract class ForwardingActor extends Actors.ExecutedQueuedActor<T> {

        protected ForwardingActor(Executor executor,
                Logger logger) {
            super(executor, new ConcurrentLinkedQueue<T>(), logger);
        }
        
        public Logger logger() {
            return logger;
        }
        
        @Override
        protected void doStop() {
            PendingQueueClientExecutor.this.doStop();
            
            T request;
            while ((request = mailbox.poll()) != null) {
                request.cancel(true);
            }
        }
    }
}
