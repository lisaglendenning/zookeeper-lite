package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;


public class ClientConnectionExecutor<C extends Connection<? super Message.ClientSession>>
    extends AbstractActor<PromiseTask<Operation.Request, Message.ServerResponse<Records.Response>>>
    implements ClientExecutor<Operation.Request, Message.ServerResponse<Records.Response>>,
        Publisher,
        Reference<C> {

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection) {
        return newInstance(
                request,
                connection,
                AssignXidProcessor.newInstance(),
                connection);
    }

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            Executor executor,
            AssignXidProcessor xids,
            C connection) {
        return newInstance(
                ConnectTask.create(connection, request),
                executor,
                xids,
                connection);
    }

    public static <C extends Connection<? super Message.ClientSession>> ClientConnectionExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            Executor executor,
            AssignXidProcessor xids,
            C connection) {
        return new ClientConnectionExecutor<C>(
                session,
                connection,
                xids,
                executor);
    }
    
    protected final Logger logger;
    protected final C connection;
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final AssignXidProcessor xids;
    protected final ConcurrentLinkedQueue<PendingMessageTask> pending;
    
    protected ClientConnectionExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            AssignXidProcessor xids,
            Executor executor) {
        super(executor, AbstractActor.<PromiseTask<Operation.Request, Message.ServerResponse<Records.Response>>>newQueue(), AbstractActor.newState());
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;
        this.xids = xids;
        this.session = session;
        this.pending = AbstractActor.newQueue();
                
        connection.register(this);
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    @Override
    public C get() {
        return connection;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(Operation.Request request) {
        return submit(request, SettableFuturePromise.<Message.ServerResponse<Records.Response>>create());
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(
            Operation.Request request, Promise<Message.ServerResponse<Records.Response>> promise) {
        PromiseTask<Operation.Request, Message.ServerResponse<Records.Response>> task = 
                PromiseTask.of(request, LoggingPromise.create(logger, promise));
        send(task);
        return task;
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Override
    public void post(Object object) {
        get().post(object);
    }

    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            stop();
        }
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse<Records.Response> message) {
        if (state() != State.TERMINATED) {
            PendingMessageTask next = pending.peek();
            if (next != null) {
                if (next.task().getXid() == message.getXid()) {
                    pending.remove(next);
                    next.set(message);
                }
            }
        }
    }

    @Override
    protected boolean apply(PromiseTask<Operation.Request, Message.ServerResponse<Records.Response>> input) throws Exception {
        if ((state() != State.TERMINATED) && ! input.isDone()) {
            Message.ClientRequest<?> message;
            try { 
                message = (Message.ClientRequest<?>) xids.apply(input.task());
            } catch (Throwable t) {
                input.setException(t);
                return super.apply(input);
            }

            // mark pings as done on write because ZooKeeper doesn't care about their ordering
            FutureCallback<Object> callback;
            if (message.getXid() == OpCodeXid.PING.getXid()) {
                callback = new SetOnCallbackTask(input);
            } else {
                // task needs to be in the queue before calling write
                PendingMessageTask task = new PendingMessageTask(message, input);
                callback = task;
                pending.add(task);
            }
            
            try {
                ListenableFuture<?> future = get().write(message);
                Futures.addCallback(future, callback);
            } catch (Throwable t) {
                callback.onFailure(t);
            }
        }
        
        return super.apply(input);
    }

    @Override
    protected void doStop() {
        super.doStop();
        
        if (! session.isDone()) {
            session.cancel(true);
        }
        
        try {
            get().unregister(this);
        } catch (IllegalArgumentException e) {}

        get().close();
        
        PendingMessageTask next = null;
        while ((next = pending.poll()) != null) {
            next.cancel(true);
        }
    }

    protected class PendingMessageTask
        extends PromiseTask<Message.ClientRequest<?>, Message.ServerResponse<Records.Response>>
        implements FutureCallback<Object> {

        public PendingMessageTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
        }

        @Override
        public boolean set(Message.ServerResponse<Records.Response> result) {
            assert (task.getXid() == result.getXid());
            return super.set(result);
        }
        
        @Override
        public void onSuccess(Object result) {
            assert (task() == result);
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
            pending.remove(this);
        }
        
        @Override
        public Promise<Message.ServerResponse<Records.Response>> delegate() {
            return delegate;
        }
    } 
    
    protected static class SetOnCallbackTask implements FutureCallback<Object> {

        protected final Promise<?> promise;
        
        public SetOnCallbackTask(Promise<?> promise) {
            this.promise = promise;
        }

        @Override
        public void onSuccess(Object result) {
            promise.set(null);
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }
    }
}
