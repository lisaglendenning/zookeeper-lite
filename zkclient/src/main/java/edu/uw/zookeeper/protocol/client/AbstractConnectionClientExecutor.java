package edu.uw.zookeeper.protocol.client;

import java.lang.ref.WeakReference;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
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
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
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


public abstract class AbstractConnectionClientExecutor<
    I extends Operation.Request, 
    V extends Message.ServerResponse<?>,
    T extends Future<?>,
    C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>,
    O>
    extends ExecutedActor<T>
    implements ConnectionClientExecutor<I,V,C>,
        FutureCallback<O> {
    
    protected static final Executor sameThreadExecutor = MoreExecutors.sameThreadExecutor();

    protected final Logger logger;
    protected final C connection;
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final AtomicReference<Throwable> failure;
    protected final TimeOutServer timeOut;
    
    protected AbstractConnectionClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService executor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;
        this.session = session;
        this.timeOut = new TimeOutServer(TimeOutParameters.create(timeOut), executor, this);
        this.failure = new AtomicReference<Throwable>(null);
                
        this.connection.register(this);
        this.timeOut.run();
        Futures.addCallback(this.session, this.timeOut, sameThreadExecutor);
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    public C connection() {
        return connection;
    }
    
    @Override
    public ListenableFuture<V> submit(I request) {
        return submit(request, SettableFuturePromise.<V>create());
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
            if (connection.codec().state() != ProtocolState.DISCONNECTED) {
                onFailure(new KeeperException.ConnectionLossException());
            } else {
                stop();
            }
        }
    }

    @Subscribe
    public void handleResponse(Message.ServerResponse<?> message) {
        timeOut.send(message);
    }
    
    @Override
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
        return Objects.toStringHelper(this).add("session", sessionStr).add("connection", connection).toString();
    }

    @Override
    protected Executor executor() {
        return connection;
    }

    @Override
    protected Logger logger() {
        return logger;
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

        T request;
        while ((request = mailbox().poll()) != null) {
            request.cancel(true);
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

        protected final WeakReference<FutureCallback<?>> callback;
        
        public TimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                FutureCallback<?> connection) {
            super(parameters, executor);
            this.callback = new WeakReference<FutureCallback<?>>(connection);
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
            FutureCallback<?> callback = this.callback.get();
            if (callback != null) {
                callback.onFailure(t);
            }
        }
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
