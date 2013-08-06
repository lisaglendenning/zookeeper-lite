package edu.uw.zookeeper.protocol.server;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Actor;
import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.ExecutorActor;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.PublisherActor;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolCodecConnection;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.proto.Records;

public class ServerConnectionExecutor<C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>>
        implements Publisher, Reference<T> {

    public static <C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> ServerConnectionExecutor<C,T> newInstance(
            T connection,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        return new ServerConnectionExecutor<C,T>(
                connection, anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    public static <C extends Connection<? super Message.Server>, T extends ProtocolCodecConnection<Message.Server, ServerProtocolCodec, C>> ParameterizedFactory<T, ServerConnectionExecutor<C,T>> factory(
            final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            final TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            final TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        return new ParameterizedFactory<T, ServerConnectionExecutor<C,T>>() {
            @Override
            public ServerConnectionExecutor<C,T> get(T connection) {
                ServerConnectionExecutor<C,T> instance = ServerConnectionExecutor.newInstance(
                        connection, 
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
    protected final TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Operation.ProtocolResponse<Records.Response>> sessionExecutor;
    protected final T connection;
    protected final InboundActor inbound;
    protected final OutboundActor outbound;
    protected final Promise<ConnectMessage.Response> session;
    
    public ServerConnectionExecutor(
            T connection,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        this.logger = LogManager.getLogger(getClass());
        this.connection = connection;
        this.anonymousExecutor = anonymousExecutor;
        this.connectExecutor = connectExecutor;
        this.sessionExecutor = sessionExecutor;
        this.session = SettableFuturePromise.create();
        this.outbound = new OutboundActor();
        this.inbound = new InboundActor();
    }

    public ListenableFuture<ConnectMessage.Response> session() {
        return session;
    }
    
    @Override
    public T get() {
        return connection;
    }
    
    public boolean stop(Throwable t) {
        boolean stopped = false;
        Actor<?>[] actors = { inbound, outbound };
        for (Actor<?> actor: actors) {
            stopped = stopped || actor.stop();
        }
        if (logger.isDebugEnabled()) {
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

    protected class InboundActor extends ExecutorActor<Message.Client> 
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
                Throwable t;
                switch (connection.codec().state()) {
                case ANONYMOUS:
                case DISCONNECTED:
                    t = null;
                    break;
                default:
                    t = new KeeperException.ConnectionLossException();
                    break;
                }
                ServerConnectionExecutor.this.stop(t);
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
            ServerConnectionExecutor.this.stop(t);
        }
    
        @Subscribe
        @Override
        public void send(Message.Client message) {
            super.send(message);
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
            if (input instanceof FourLetterRequest) {
                Futures.addCallback(anonymousExecutor.submit((FourLetterRequest) input), this);
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
    }

    protected class OutboundActor extends PublisherActor 
        implements FutureCallback<Object> {
    
        public OutboundActor() {
            super(connection, connection, new ConcurrentLinkedQueue<Object>());
        }

        @Override
        public void onSuccess(Object result) {
        }

        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.stop(t);
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
                ServerConnectionExecutor.this.stop(new KeeperException.SessionExpiredException());
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
