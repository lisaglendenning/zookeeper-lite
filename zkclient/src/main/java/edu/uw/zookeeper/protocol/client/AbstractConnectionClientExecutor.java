package edu.uw.zookeeper.protocol.client;

import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Automatons.AutomatonListener;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Operation.ProtocolResponse;
import edu.uw.zookeeper.protocol.ProtocolConnection;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;


public abstract class AbstractConnectionClientExecutor<
    I extends Operation.Request, 
    V extends Operation.ProtocolResponse<?>,
    T extends Future<?>,
    C extends ProtocolConnection<? super Message.ClientSession, ? extends Operation.Response,?,?,?>, 
    O>
    implements ConnectionClientExecutor<I,V,SessionListener,C>, Connection.Listener<Operation.Response>,
        FutureCallback<O>, AutomatonListener<ProtocolState>, Actor<T> {

    public static <I extends Operation.Request, T extends ConnectionClientExecutor<I,?,?,?>> void disconnect(T client) throws InterruptedException, ExecutionException, TimeoutException, KeeperException {
        Disconnect<I,T> instance = Disconnect.create(client);
        if (client.session().isDone()) {
            instance.get(client.session().get().getTimeOut(), TimeUnit.MILLISECONDS);
        } else {
            instance.get();
        }
    }
    
    public static final class Disconnect<I extends Operation.Request, T extends ConnectionClientExecutor<I,?,?,?>> extends PromiseTask<T, Boolean> implements Automatons.AutomatonListener<ProtocolState>, Connection.Listener<Object>, Runnable, FutureCallback<Operation.ProtocolResponse<?>> {

        public static <I extends Operation.Request, T extends ConnectionClientExecutor<I,?,?,?>> Disconnect<I,T> create(
                T client) {
            Disconnect<I,T> instance = new Disconnect<I,T>(client, SettableFuturePromise.<Boolean>create());
            instance.run();
            return instance;
        }
        
        protected Disconnect(
                T client,
                Promise<Boolean> promise) {
            super(client, promise);
            task().connection().subscribe(this);
            task().connection().codec().subscribe(this);
            addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public void run() {
            if (isDone()) {
                task().connection().unsubscribe(this);
                task().connection().codec().unsubscribe(this);
            } else if (task().session().isDone()) {
                ProtocolState state = task().connection().codec().state();
                handleAutomatonTransition(Automaton.Transition.create(state, state));
            } else {
                task().session().addListener(this, MoreExecutors.directExecutor());
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            switch (transition.to()) {
            case CONNECTED:
                if (transition.from() == transition.to()) {
                    Futures.addCallback(
                            task().submit(
                                    (I) ProtocolRequestMessage.of(
                                            0, 
                                            Operations.Requests.disconnect().build())),
                            this);
                }
                break;
            case DISCONNECTED:
                set(Boolean.TRUE);
                break;
            case ERROR:
                onFailure(new KeeperException.SessionExpiredException());
                break;
            default:
            {
                if (transition.from() == transition.to()) {
                    Connection.State state = task().connection().state();
                    handleConnectionState(Automaton.Transition.create(state, state));
                }
                break;
            }
            }
        }

        @Override
        public void handleConnectionState(
                Automaton.Transition<Connection.State> state) {
            if (state.to() == Connection.State.CONNECTION_CLOSED) {
                setException(new ClosedChannelException());
            }
        }

        @Override
        public void handleConnectionRead(Object message) {
        }

        @Override
        public void onSuccess(Operation.ProtocolResponse<?> result) {
            if (result.record().opcode() == OpCode.CLOSE_SESSION) {
                set(Boolean.TRUE);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }
    
    protected final ListenableFuture<ConnectMessage.Response> session;
    protected final C connection;
    protected final Listeners listeners;
    protected final TimeOutServer<Operation.Response> timer;
    protected final AtomicReference<Throwable> failure;
    
    protected AbstractConnectionClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            TimeValue timeOut,
            ScheduledExecutorService scheduler) {
        this(session, connection, Listeners.create(), TimeOutServer.<Operation.Response>newTimeOutServer(TimeOutParameters.milliseconds(timeOut.value(TimeUnit.MILLISECONDS)), scheduler), new AtomicReference<Throwable>());
    }
    
    protected AbstractConnectionClientExecutor(
            ListenableFuture<ConnectMessage.Response> session,
            C connection,
            Listeners listeners,
            TimeOutServer<Operation.Response> timer,
            AtomicReference<Throwable> failure) {
        this.session = session;
        this.connection = connection;
        this.listeners = listeners;
        this.timer = timer;
        this.failure = failure;

        new TimeOutListener();
        Futures.addCallback(this.session, this.timer);
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
        listeners.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return listeners.unsubscribe(listener);
    }

    @Override
    public ListenableFuture<V> submit(I request) {
        return submit(request, SettableFuturePromise.<V>create());
    }
    
    @Override
    public boolean send(T input) {
        return actor().send(input);
    }
    
    @Override
    public Actor.State state() {
        return actor().state();
    }
    
    @Override
    public boolean stop() {
        return actor().stop();
    }
    
    @Override
    public void run() {
        actor().run();
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
        logger().debug("Read {} ({})", message, this);
        timer.send(message);
        if (message instanceof Operation.ProtocolResponse<?>) {
            if (((Operation.ProtocolResponse<?>) message).xid() == OpCodeXid.NOTIFICATION.xid()) {
                listeners.handleNotification((Operation.ProtocolResponse<IWatcherEvent>) message);
            }
        }
    }
    
    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        listeners.handleAutomatonTransition(transition);
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
        return toStringHelper().toString();
    }
    
    protected abstract Actor<? super T> actor();
    
    protected abstract Logger logger();

    protected void doStop() {  
        timer.cancel(true);
        
        connection.unsubscribe(this);
        connection.codec().unsubscribe(this);
        
        session.cancel(false);
        
        connection.close();

        listeners.clear();
    }
    
    protected MoreObjects.ToStringHelper toStringHelper() {
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
        return MoreObjects.toStringHelper(this).add("session", sessionStr).add("connection", connection);
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
    
    public static class Listeners implements Eventful<SessionListener>, SessionListener {
        
        public static Listeners create() {
            return new Listeners(new StrongConcurrentSet<SessionListener>());
        }
        
        protected final IConcurrentSet<SessionListener> listeners;
        
        public Listeners(IConcurrentSet<SessionListener> listeners) {
            this.listeners = listeners;
        }
        
        public void clear() {
            Iterator<?> itr = Iterators.consumingIterator(listeners.iterator());
            while (itr.hasNext()) {
                itr.next();
            }
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
        public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
            for (SessionListener listener: listeners) {
                listener.handleAutomatonTransition(transition);
            }
        }

        @Override
        public void handleNotification(
                ProtocolResponse<IWatcherEvent> notification) {
            for (SessionListener listener: listeners) {
                listener.handleNotification(notification);
            }
        }
        
    }
    
    protected class TimeOutListener implements Runnable {

        public TimeOutListener() {
            timer.addListener(this, MoreExecutors.directExecutor());
        }
        
        @Override
        public void run() {
            if (timer.isDone()) {
                if (!timer.isCancelled()) {
                    try {
                        timer.get();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        onFailure(e.getCause());
                    }
                } else {
                    stop();
                }
            }
        }
    }
    
    public static class TimeOutServer<V> extends TimeOutActor<Operation.Response, V> implements FutureCallback<ConnectMessage.Response> {

        public static <V>TimeOutServer<V> newTimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService scheduler) {
            Logger logger = LogManager.getLogger(TimeOutServer.class);
            TimeOutServer<V> timeOut = new TimeOutServer<V>(
                    parameters, 
                    scheduler,
                    Sets.<Pair<Runnable,Executor>>newHashSet(),
                    SettableFuturePromise.<V>create(),
                    logger);
            LoggingFutureListener.listen(logger, timeOut);
            return timeOut;
        }
        
        public TimeOutServer(
                TimeOutParameters parameters,
                ScheduledExecutorService scheduler,
                Set<Pair<Runnable,Executor>> listeners,
                Promise<V> promise,
                Logger logger) {
            super(parameters, scheduler, listeners, promise, logger);
        }

        @Override
        public void onSuccess(ConnectMessage.Response result) {
            if (result instanceof ConnectMessage.Response.Valid) {
                synchronized (this) {
                    parameters.setTimeOut(((ConnectMessage.Response) result).getTimeOut());
                }
            }
            
            send(result);
        }

        @Override
        public void onFailure(Throwable t) {
            promise.setException(t);
        }
    }
}
