package edu.uw.zookeeper.protocol.client;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons.AutomatonListener;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;


public abstract class AbstractConnectionClientExecutor<
    I extends Operation.Request, 
    V extends Operation.ProtocolResponse<?>,
    T extends Future<?>,
    C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>, 
    O>
    implements ConnectionClientExecutor<I,V,SessionListener,C>, Connection.Listener<Operation.Response>,
        FutureCallback<O>, AutomatonListener<ProtocolState>, Actor<T> {
    
    protected static final Executor SAME_THREAD_EXECUTOR = MoreExecutors.sameThreadExecutor();

    protected final C connection;
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final TimeOutServer<O> timer;
    protected final IConcurrentSet<SessionListener> listeners;
    protected final AtomicReference<Throwable> failure;
    
    protected AbstractConnectionClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler,
            IConcurrentSet<SessionListener> listeners) {
        this.connection = connection;
        this.session = session;
        this.listeners = listeners;
        this.timer = TimeOutServer.newTimeOutServer(TimeOutParameters.create(timeOut), scheduler);
        this.failure = new AtomicReference<Throwable>();

        new TimeOutListener();
        Futures.addCallback(this.session, this.timer, SAME_THREAD_EXECUTOR);
        this.connection.subscribe(this);
        this.connection.codec().subscribe(this);
        this.timer.run();
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    public C connection() {
        return connection;
    }

    @Override
    public void subscribe(SessionListener listener) {
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return listeners.remove(listener);
    }

    @Override
    public ListenableFuture<V> submit(I request) {
        return submit(request, SettableFuturePromise.<V>create());
    }

    @Override
    public void handleConnectionState(Automaton.Transition<Connection.State> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            switch (connection.codec().state()) {
            case CONNECTING:
            case CONNECTED:
            case DISCONNECTING:
                onFailure(new KeeperException.ConnectionLossException());
                break;
            case ERROR:
                onFailure(new KeeperException.SessionExpiredException());
            default:
                stop();
                break;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleConnectionRead(Operation.Response message) {
        logger().debug("Received: {}", message);
        timer.send(message);
        if (message instanceof Operation.ProtocolResponse<?>) {
            if (((Operation.ProtocolResponse<?>) message).record().opcode() == OpCode.NOTIFICATION) {
                for (SessionListener listener: listeners) {
                    listener.handleNotification((Operation.ProtocolResponse<IWatcherEvent>) message);
                }
            }
        }
    }
    
    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        for (SessionListener listener: listeners) {
            listener.handleAutomatonTransition(transition);
        }
    }

    @Override
    public void onSuccess(O result) {
    }

    @Override
    public void onFailure(Throwable t) {
        if ((state().compareTo(State.TERMINATED) < 0) && failure.compareAndSet(null, t)) {
            logger().debug("{}", this, t);
            stop();
        }
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
    
    protected abstract Logger logger();

    protected void doStop() {  
        timer.cancel(true);
        
        connection.unsubscribe(this);
        connection.codec().unsubscribe(this);
        
        if (! session.isDone()) {
            session.cancel(true);
        }
        
        connection.close();

        Iterator<?> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            itr.next();
        }
    }
    
    protected class TimeOutListener implements Runnable {

        public TimeOutListener() {
            timer.addListener(this, SAME_THREAD_EXECUTOR);
        }
        
        @Override
        public void run() {
            if (timer.isDone()) {
                if (! timer.isCancelled()) {
                    try {
                        timer.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        onFailure(e.getCause());
                    }
                }
            }
        }
    }
    
    public static class TimeOutServer<V> extends TimeOutActor<Operation.Response, V> implements FutureCallback<ConnectMessage.Response> {

        public static <V>TimeOutServer<V> newTimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService executor) {
            Logger logger = LogManager.getLogger(TimeOutServer.class);
            return new TimeOutServer<V>(
                    parameters, 
                    executor,
                    new WeakConcurrentSet<Pair<Runnable,Executor>>(),
                    LoggingPromise.create(logger, SettableFuturePromise.<V>create()),
                    logger);
        }
        
        public TimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService executor,
                IConcurrentSet<Pair<Runnable,Executor>> listeners,
                Promise<V> promise,
                Logger logger) {
            super(parameters, executor, listeners, promise, logger);
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
            promise.setException(t);
        }
    }
}
