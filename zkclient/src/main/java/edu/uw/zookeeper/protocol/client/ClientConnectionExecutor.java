package edu.uw.zookeeper.protocol.client;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.ForwardingPromise;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodec;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public class ClientConnectionExecutor<C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>>
    extends ExecutedActor<PromiseTask<Operation.Request, Message.ServerResponse<?>>>
    implements ClientExecutor<Operation.Request, Message.ServerResponse<?>>,
        Publisher,
        Reference<C> {

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                request,
                AssignXidProcessor.newInstance(),
                connection,
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ClientConnectionExecutor<C> newInstance(
            ConnectMessage.Request request,
            AssignXidProcessor xids,
            C connection,
            ScheduledExecutorService executor) {
        return newInstance(
                ConnectTask.create(connection, request),
                xids,
                connection,
                TimeValue.create(Long.valueOf(request.getTimeOut()), TimeUnit.MILLISECONDS),
                executor);
    }

    public static <C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>> ClientConnectionExecutor<C> newInstance(
            ListenableFuture<ConnectMessage.Response> session,
            AssignXidProcessor xids,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        return new ClientConnectionExecutor<C>(
                session,
                xids,
                connection,
                timeOut,
                executor);
    }
    
    protected final Logger logger;
    protected final C connection;
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final AssignXidProcessor xids;
    protected final ConcurrentLinkedQueue<PromiseTask<Operation.Request, Message.ServerResponse<?>>> mailbox;
    protected final ConcurrentLinkedQueue<PendingResponseTask> pending;
    protected final AtomicReference<Throwable> failure;
    protected final TimeOutServer timeOut;
    
    protected ClientConnectionExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            AssignXidProcessor xids,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;
        this.xids = xids;
        this.session = session;
        this.timeOut = new TimeOutServer(TimeOutParameters.create(timeOut), executor, this);
        this.failure = new AtomicReference<Throwable>(null);
        this.pending = new ConcurrentLinkedQueue<PendingResponseTask>();
        this.mailbox = new ConcurrentLinkedQueue<PromiseTask<Operation.Request, Message.ServerResponse<?>>>();
                
        this.connection.register(this);
        this.timeOut.run();
        Futures.addCallback(this.session, this.timeOut, executor());
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    @Override
    public C get() {
        return connection;
    }
    
    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(Operation.Request request) {
        return submit(request, SettableFuturePromise.<Message.ServerResponse<?>>create());
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(
            Operation.Request request, Promise<Message.ServerResponse<?>> promise) {
        PromiseTask<Operation.Request, Message.ServerResponse<?>> task = 
                PromiseTask.of(request, LoggingPromise.create(logger, promise));
        if (! send(task)) {
            task.cancel(true);
        }
        return task;
    }

    @Override
    public void register(Object object) {
        connection.register(object);
    }

    @Override
    public void unregister(Object object) {
        connection.unregister(object);
    }

    @Override
    public void post(Object object) {
        connection.post(object);
    }

    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            if (get().codec().state() != ProtocolState.DISCONNECTED) {
                onFailure(new KeeperException.ConnectionLossException());
            } else {
                stop();
            }
        }
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) {
        if (state() != State.TERMINATED) {
            timeOut.send(message);
            int xid = message.xid();
            if (! ((xid == OpCodeXid.PING.xid()) || (xid == OpCodeXid.NOTIFICATION.xid()))) {
                PendingResponseTask next = pending.peek();
                if ((next != null) && (next.getXid() == xid)) {
                    pending.remove(next);
                    next.set(message);
                } else {
                    // FIXME is this an error?
                    logger.warn("{} != {} ({})", next, message, this);
                }
            }
        }
    }
    
    public void onFailure(Throwable t) {
        failure.compareAndSet(null, t);
        stop();
    }
    
    @Override
    public String toString() {
        String sessionStr = null;
        if (session().isDone()) {
            if (session.isCancelled()) {
                sessionStr = "cancelled";
            } else {
                try {
                    sessionStr = Session.toString(session().get().getSessionId());
                } catch (Exception e) {
                    sessionStr = e.toString();
                }
            }
        }
        return Objects.toStringHelper(this).add("session", sessionStr).add("connection", get()).toString();
    }

    @Override
    protected Executor executor() {
        return connection;
    }

    @Override
    protected ConcurrentLinkedQueue<PromiseTask<Operation.Request, Message.ServerResponse<?>>> mailbox() {
        return mailbox;
    }

    @Override
    protected Logger logger() {
        return logger;
    }
    
    @Override
    protected boolean apply(PromiseTask<Operation.Request, Message.ServerResponse<?>> input) {
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                Message.ClientRequest<?> message = (Message.ClientRequest<?>) xids.apply(input.task());
    
                // mark pings as done on write because ZooKeeper doesn't care about their ordering
                PendingTask task;
                if (message.xid() == OpCodeXid.PING.xid()) {
                    task = new SetOnCallbackTask(message.xid(), input);
                } else {
                    // task needs to be in the queue before calling write
                    PendingResponseTask p = new PendingResponseTask(message.xid(), input);
                    task = p;
                    pending.add(p);
                }
                
                try {
                    ListenableFuture<?> writeFuture = connection.write(message);
                    Futures.addCallback(writeFuture, task, executor());
                } catch (Throwable t) {
                    task.onFailure(t);
                }
            } else {
                input.cancel(true);
            }
        }
        
        return (state() != State.TERMINATED);
    }

    @Override
    protected void doStop() {  
        timeOut.stop();
        
        try {
            connection.unregister(this);
        } catch (Exception e) {}
        
        if (! session.isDone()) {
            session.cancel(true);
        }

        PromiseTask<Operation.Request, Message.ServerResponse<?>> request;
        while ((request = mailbox.poll()) != null) {
            request.cancel(true);
        }

        Throwable failure = this.failure.get();
        PendingResponseTask task;
        while ((task = pending.poll()) != null) {
            if (failure == null) {
                task.cancel(true);
            } else {
                task.setException(failure);
            }
        }
        
        try {
            connection.close().get();
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            logger.debug("Ignoring {}", e);
        }
    }
    
    protected static class TimeOutServer extends TimeOutActor<Message.Server> implements FutureCallback<ConnectMessage.Response> {

        protected final WeakReference<ClientConnectionExecutor<?>> connection;
        
        public TimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                ClientConnectionExecutor<?> connection) {
            super(parameters, executor);
            this.connection = new WeakReference<ClientConnectionExecutor<?>>(connection);
        }

        @Override
        protected void doRun() {
            if (parameters.remaining() <= 0) {
                onFailure(new KeeperException.OperationTimeoutException());
            }
        }

        @Override
        public void onSuccess(ConnectMessage.Response result) {
            if (result instanceof ConnectMessage.Response.Valid) {
                parameters.setTimeOut(((ConnectMessage.Response) result).toParameters().timeOut().value());
            }
            send(result);
        }

        @Override
        public void onFailure(Throwable t) {
            ClientConnectionExecutor<?> connection = this.connection.get();
            if (connection != null) {
                connection.onFailure(t);
            }
        }
    }

    protected abstract class PendingTask
        extends ForwardingPromise<Message.ServerResponse<?>>
        implements FutureCallback<Object> {

        protected final int xid;
        protected final Promise<Message.ServerResponse<?>> promise;
        
        protected PendingTask(
                int xid,
                Promise<Message.ServerResponse<?>> promise) {
            this.xid = xid;
            this.promise = promise;
        }
        
        public int getXid() {
            return xid;
        }
        
        @Override
        public void onSuccess(Object result) {
        }
        
        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
        
        @Override
        public Promise<Message.ServerResponse<?>> delegate() {
            return promise;
        }
    } 
    
    protected class PendingResponseTask extends PendingTask {

        public PendingResponseTask(
                int xid,
                Promise<Message.ServerResponse<?>> promise) {
            super(xid, promise);
        }

        @Override
        public boolean set(Message.ServerResponse<?> result) {
            assert (getXid() == result.xid());
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
    
    protected class SetOnCallbackTask extends PendingTask {

        public SetOnCallbackTask(
                int xid,
                Promise<Message.ServerResponse<?>> promise) {
            super(xid, promise);
        }

        @Override
        public void onSuccess(Object result) {
            set(null);
        }
    }
}
