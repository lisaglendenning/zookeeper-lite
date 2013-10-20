package edu.uw.zookeeper.protocol.client;

import java.lang.ref.WeakReference;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import net.engio.mbassy.listener.Handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
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
    V extends Operation.ProtocolResponse<?>,
    T extends Future<?>,
    C extends ProtocolCodecConnection<? super Message.ClientSession, ? extends ProtocolCodec<?,?>, ?>, O>
    extends ExecutedActor<T>
    implements ConnectionClientExecutor<I,V,C>,
        FutureCallback<O> {
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

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
                
        this.connection.subscribe(this);
        this.timeOut.run();
        Futures.addCallback(this.session, this.timeOut, SAME_THREAD_EXECUTOR);
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    public C connection() {
        return connection;
    }

    @Override
    public void subscribe(Object listener) {
        connection.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return connection.unsubscribe(listener);
    }

    @Override
    public void publish(Object message) {
        connection.publish(message);
    }
    
    @Override
    public ListenableFuture<V> submit(I request) {
        return submit(request, SettableFuturePromise.<V>create());
    }

    @Handler
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            if (connection.codec().state() != ProtocolState.DISCONNECTED) {
                onFailure(new KeeperException.ConnectionLossException());
            } else {
                stop();
            }
        }
    }

    @Handler
    public void handleResponse(Operation.ProtocolResponse<?> message) {
        logger.debug("Received: {}", message);
        timeOut.send(message);
    }

    @Override
    public void onSuccess(O result) {
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
            connection.unsubscribe(this);
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
        } catch (Exception e) {
            logger.debug("Ignoring {}", e);
        }
    }
    
    protected static class TimeOutServer extends TimeOutActor<Operation.Response> implements FutureCallback<ConnectMessage.Response> {

        protected final Logger logger;
        protected final WeakReference<FutureCallback<?>> callback;
        
        public TimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                FutureCallback<?> connection) {
            super(parameters, executor);
            this.callback = new WeakReference<FutureCallback<?>>(connection);
            this.logger = LogManager.getLogger(getClass());
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

        @Override
        protected void doRun() {
            if (parameters.remaining() <= 0) {
                onFailure(new KeeperException.OperationTimeoutException());
            }
        }

        @Override
        protected synchronized void doSchedule() {
            if (callback.get() == null) {
                stop();
            } else {
                super.doSchedule();
            }
        }
        
        @Override
        protected Logger logger() {
            return logger;
        }
    }
}
