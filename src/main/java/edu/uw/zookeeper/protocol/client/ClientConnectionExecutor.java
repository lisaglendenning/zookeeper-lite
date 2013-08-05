package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
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
    protected final ConcurrentLinkedQueue<PendingResponseTask> pending;
    
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
            PendingResponseTask next = pending.peek();
            if ((next != null) && (next.task().getXid() == message.getXid())) {
                pending.remove(next);
                next.set(message);
            } else {
                logger.debug("{} != {} ({})", next, message, this);
            }
        }
    }
    
    @Override
    public String toString() {
        String sessionStr = null;
        if (session().isDone()) {
            try {
                sessionStr = Session.toString(session().get().getSessionId());
            } catch (CancellationException e) {
                sessionStr = "cancelled";
            } catch (Exception e) {
                sessionStr = e.toString();
            }
        }
        return Objects.toStringHelper(this).add("session", sessionStr).add("connection", get()).toString();
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
            MessageTask task;
            if (message.getXid() == OpCodeXid.PING.getXid()) {
                task = new SetOnCallbackTask(message, input);
            } else {
                // task needs to be in the queue before calling write
                PendingResponseTask p = new PendingResponseTask(message, input);
                task = p;
                pending.add(p);
            }
            task.call();
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
        
        PendingResponseTask next = null;
        while ((next = pending.poll()) != null) {
            next.cancel(true);
        }
    }

    protected abstract class MessageTask
        extends PromiseTask<Message.ClientRequest<?>, Message.ServerResponse<Records.Response>>
        implements FutureCallback<Object>, Callable<ListenableFuture<?>> {

        protected volatile ListenableFuture<?> writeFuture;
        
        public MessageTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
            this.writeFuture = null;
        }
        
        @Override
        public ListenableFuture<?> call() {
            try {
                writeFuture = ClientConnectionExecutor.this.get().write(task());
                Futures.addCallback(writeFuture, this);
            } catch (Throwable t) {
                onFailure(t);
            }
            return writeFuture;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean doCancel = super.cancel(mayInterruptIfRunning);
            if (doCancel) {
                if (writeFuture != null) {
                    writeFuture.cancel(mayInterruptIfRunning);
                }
            }
            return doCancel;
        }

        @Override
        public void onSuccess(Object result) {
            assert (task() == result);
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
        
        @Override
        public Promise<Message.ServerResponse<Records.Response>> delegate() {
            return delegate;
        }
    } 
    
    protected class PendingResponseTask extends MessageTask {

        public PendingResponseTask(
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
        public boolean setException(Throwable t) {
            boolean doSet = super.setException(t);
            if (doSet) {
                pending.remove(this);
            }
            return doSet;
        }
    } 
    
    protected class SetOnCallbackTask extends MessageTask {

        public SetOnCallbackTask(
                Message.ClientRequest<?> task,
                Promise<Message.ServerResponse<Records.Response>> delegate) {
            super(task, delegate);
        }

        @Override
        public void onSuccess(Object result) {
            set(null);
        }
    }
}
