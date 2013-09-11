package edu.uw.zookeeper.protocol.server;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ActorPublisher;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.TelnetCloseRequest;
import edu.uw.zookeeper.protocol.TimeOutActor;
import edu.uw.zookeeper.protocol.TimeOutParameters;
import edu.uw.zookeeper.protocol.proto.Records;

public class ServerConnectionExecutor<T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>>
        implements Publisher, Reference<T> {

    public static <T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> ServerConnectionExecutor<T> newInstance(
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> sessionExecutor) {
        return new ServerConnectionExecutor<T>(
                connection, timeOut, executor, anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    public static <T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, ?>> ParameterizedFactory<T, ServerConnectionExecutor<T>> factory(
            final TimeValue timeOut,
            final ScheduledExecutorService executor,
            final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            final TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            final TaskExecutor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> sessionExecutor) {
        return new ParameterizedFactory<T, ServerConnectionExecutor<T>>() {
            @Override
            public ServerConnectionExecutor<T> get(T connection) {
                ServerConnectionExecutor<T> instance = ServerConnectionExecutor.newInstance(
                        connection, 
                        timeOut,
                        executor,
                        anonymousExecutor, 
                        connectExecutor, 
                        sessionExecutor);
                return instance;
            }
        };
    }

    protected final Logger logger;
    protected final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor;
    protected final TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor;
    protected final TaskExecutor<? super SessionOperation.Request<?>, ? extends Operation.ProtocolResponse<?>> sessionExecutor;
    protected final T connection;
    protected final InboundActor inbound;
    protected final OutboundActor outbound;
    protected final Promise<ConnectMessage.Response> session;
    protected final AtomicReference<Throwable> failure;
    protected final TimeOutClient timeOut;
    
    public ServerConnectionExecutor(
            T connection,
            TimeValue timeOut,
            ScheduledExecutorService executor,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> sessionExecutor) {
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;
        this.anonymousExecutor = anonymousExecutor;
        this.connectExecutor = connectExecutor;
        this.sessionExecutor = sessionExecutor;
        this.session = SettableFuturePromise.create();
        this.failure = new AtomicReference<Throwable>(null);
        this.timeOut = new TimeOutClient(TimeOutParameters.create(timeOut), executor, this);
        this.outbound = new OutboundActor();
        this.inbound = new InboundActor();
        
        Futures.addCallback(this.session, this.timeOut);
        this.timeOut.run();
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    @Override
    public T get() {
        return connection;
    }

    @Override
    public void post(Object event) {
        outbound.post(event);
    }

    @Override
    public void register(Object handler) {
        outbound.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        outbound.unregister(handler);
    }
    
    public void onFailure(Throwable t) {
        failure.compareAndSet(null, t);
        stop();
    }
    
    public synchronized boolean stop() {
        boolean stopped = false;
        Actor<?>[] actors = { inbound, outbound, timeOut };
        for (Actor<?> actor: actors) {
            stopped = actor.stop() || stopped;
        }
        if (logger.isDebugEnabled()) {
            Throwable t = failure.get();
            if (stopped) {
                if (t != null) {
                    logger.debug("ERROR {}", this, t);
                } else {
                    logger.debug("DISCONNECTED {}", this);
                }
            } else if (t != null) {
                logger.debug("Ignoring {} {}", this, t);
            }
        }
        return stopped;
    }
    
    @Override
    public String toString() {
        long sessionId = Session.UNINITIALIZED_ID;
        if (session.isDone()) {
            try {
                sessionId = session.get().getSessionId();
            } catch (Exception e) {}
        }
        return Objects.toStringHelper(this)
                .add("session", Session.toString(sessionId))
                .add("connection", get())
                .toString();
    }
    
    protected static class TimeOutClient extends TimeOutActor<Message.Client> implements FutureCallback<ConnectMessage.Response> {

        protected final WeakReference<ServerConnectionExecutor<?>> connection;
        
        public TimeOutClient(TimeOutParameters parameters,
                ScheduledExecutorService executor,
                ServerConnectionExecutor<?> connection) {
            super(parameters, executor);
            this.connection = new WeakReference<ServerConnectionExecutor<?>>(connection);
        }

        @Override
        protected void doRun() {
            if (parameters.remaining() <= 0) {
                onFailure(new KeeperException.OperationTimeoutException());
            }
        }

        @Override
        public void onSuccess(ConnectMessage.Response result) {
            stop();
        }

        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor<?> connection = this.connection.get();
            if (connection != null) {
                connection.onFailure(t);
            }
        }
    }

    protected class InboundActor extends ExecutedActor<Message.Client> 
            implements FutureCallback<Object> {
    
        protected final ConcurrentLinkedQueue<Message.Client> mailbox;
        protected volatile boolean throttled;
        
        public InboundActor() {
            super();
            this.mailbox = new ConcurrentLinkedQueue<Message.Client>();
            this.throttled = false;
            
            connection.register(this);
        }
        
        public void throttle(boolean throttled) {
            this.throttled = throttled;
            if (! throttled) {
                run();
            }
        }
    
        @Subscribe
        public void handleTransitionEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                switch (connection.codec().state()) {
                case ANONYMOUS:
                case DISCONNECTED:
                    ServerConnectionExecutor.this.stop();
                    break;
                default:
                    ServerConnectionExecutor.this.onFailure(new KeeperException.ConnectionLossException());
                    break;
                }
            }
        }

        @Override
        public void onSuccess(Object result) {  
            if (result instanceof FourLetterResponse) {
                outbound.post(result);
            } else if (result instanceof ConnectMessage.Response) {
                session.set((ConnectMessage.Response) result);
                if (result instanceof ConnectMessage.Response.Valid) {
                    throttle(false);
                } else {
                    // if the response is Invalid, we want the response
                    // to be flushed to the client before closing the connection
                }
            }
        }

        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.onFailure(t);
        }
    
        @Subscribe
        public void handleMessage(Message.Client message) {
            timeOut.send(message);
            send(message);
        }
        
        @Override
        protected boolean schedule() {
            if (throttled) {
                return false;
            } else {
                return super.schedule();
            }
        }
        
        @Override
        protected void doRun() {
            Message.Client next;
            while (!throttled && ((next = mailbox.poll()) != null)) {
                if (! apply(next)) {
                    break;
                }
            }
        }
    
        @Override
        protected boolean apply(Message.Client input) {
            // ordering constraint: requests are submitted in the same
            // order that they are received
            if (input instanceof Message.ClientAnonymous) {
                if (input instanceof FourLetterRequest) {
                    Futures.addCallback(anonymousExecutor.submit((FourLetterRequest) input), this);
                } else if (input instanceof TelnetCloseRequest) {
                    ServerConnectionExecutor.this.stop();
                } else {
                    throw new AssertionError(String.valueOf(input));
                }
            } else if (input instanceof ConnectMessage.Request) {
                throttle(true);
                Futures.addCallback(connectExecutor.submit(Pair.create((ConnectMessage.Request) input, (Publisher) outbound)), this);
            } else {
                @SuppressWarnings("unchecked")
                Message.ClientRequest<Records.Request> request = (Message.ClientRequest<Records.Request>) input;
                long sessionId = Futures.getUnchecked(session()).getSessionId();
                Futures.addCallback(sessionExecutor.submit(SessionRequest.of(sessionId, request, request)), this);
            }
            
            return (state() != State.TERMINATED);
        }
    
        @Override
        protected void doStop() {
            try {
                connection.unregister(this);
            } catch (Exception e) {}
            
            if (logger.isDebugEnabled()) {
                Message.Client request;
                while ((request = mailbox.poll()) != null) {
                    logger.debug("DROPPING {} ({})", request, ServerConnectionExecutor.this);
                }
            }
            mailbox.clear();
        }

        @Override
        protected Executor executor() {
            return connection;
        }

        @Override
        protected ConcurrentLinkedQueue<Message.Client> mailbox() {
            return mailbox;
        }

        @Override
        protected Logger logger() {
            return logger;
        }
    }

    protected class OutboundActor extends ActorPublisher 
        implements FutureCallback<Object> {
    
        public OutboundActor() {
            super(connection, connection, new ConcurrentLinkedQueue<Object>(), ServerConnectionExecutor.this.logger);
        }

        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.onFailure(t);
        }
        
        @Override
        protected boolean apply(Object input) {
            // ordering constraint: messages are written in the order
            // that they were enqueued in outbound
            if (input instanceof Message.Server) {
                switch (connection.state()) {
                case CONNECTION_OPENING:
                case CONNECTION_OPENED:
                    try {
                        Futures.addCallback(connection.write((Message.Server) input), this);
                    } catch (Throwable t) {
                        onFailure(t);
                    }
                    break;
                default:
                    logger.debug("DROPPING {} ({})", input, ServerConnectionExecutor.this);
                    break;
                }
            } else if (Session.State.SESSION_EXPIRED == input) {
                ServerConnectionExecutor.this.onFailure(new KeeperException.SessionExpiredException());
            }
            
            return super.apply(input);
        }
    
        @Override
        protected void doStop() {
            super.doStop();
            
            switch (connection.state()) {
            case CONNECTION_OPENING:
            case CONNECTION_OPENED:
            {
                connection.close();
                break;
            }
            default:
                break;
            }
        }
    }
}
