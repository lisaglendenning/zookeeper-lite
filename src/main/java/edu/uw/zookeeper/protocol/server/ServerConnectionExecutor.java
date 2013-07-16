package edu.uw.zookeeper.protocol.server;

import java.util.Queue;
import java.util.concurrent.Executor;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ForwardingQueue;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.FourLetterRequest;
import edu.uw.zookeeper.protocol.FourLetterResponse;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Actor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TaskExecutor;

public class ServerConnectionExecutor<C extends Connection<Message.Server>>
        implements Publisher, Reference<C>, FutureCallback<Object> {

    public static <C extends Connection<Message.Server>> ServerConnectionExecutor<C> newInstance(
            C connection,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        return new ServerConnectionExecutor<C>(
                connection, connection, connection, anonymousExecutor, connectExecutor, sessionExecutor);
    }
    
    public static <C extends Connection<Message.Server>> ParameterizedFactory<C, ServerConnectionExecutor<C>> factory(
            final TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            final TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            final TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        return new ParameterizedFactory<C, ServerConnectionExecutor<C>>() {
            @Override
            public ServerConnectionExecutor<C> get(C connection) {
                ServerConnectionExecutor<C> instance = ServerConnectionExecutor.newInstance(
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
    protected final C connection;
    protected final InboundActor inbound;
    protected final OutboundActor outbound;
    protected volatile Session session;
    
    public ServerConnectionExecutor(
            Publisher publisher,
            Executor executor,
            C connection,
            TaskExecutor<? super FourLetterRequest, ? extends FourLetterResponse> anonymousExecutor,
            TaskExecutor<? super Pair<ConnectMessage.Request, Publisher>, ? extends ConnectMessage.Response> connectExecutor,
            TaskExecutor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> sessionExecutor) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.connection = connection;
        this.anonymousExecutor = anonymousExecutor;
        this.connectExecutor = connectExecutor;
        this.sessionExecutor = sessionExecutor;
        this.session = Session.uninitialized();
        this.outbound = new OutboundActor(publisher, executor);
        this.inbound = new InboundActor(executor);
    }
    
    @Override
    public C get() {
        return connection;
    }
    
    @Override
    public void onSuccess(Object result) {
    }

    @Override
    public void onFailure(Throwable t) {
        logger.debug("Stopping", t);
        
        Actor<?>[] actors = { inbound, outbound };
        for (Actor<?> actor: actors) {
            actor.stop();
        }
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
    
    protected static class ThrottlableMailbox<T> extends ForwardingQueue<T> {
    
        public static <T> ThrottlableMailbox<T> newInstance() {
            return new ThrottlableMailbox<T>(AbstractActor.<T>newQueue());
        }
        
        protected final Queue<T> delegate;
        protected volatile boolean throttled;
        
        public ThrottlableMailbox(Queue<T> delegate) {
            this.delegate = delegate;
            this.throttled = false;
        }
        
        public boolean throttled() {
            return throttled;
        }
        
        public void throttle(boolean throttled) {
            this.throttled = throttled;
        }
    
        @Override
        public Queue<T> delegate() {
            return delegate;
        }
        
        @Override
        public T poll() {
            return  throttled ? null : super.poll();
        }
    
        @Override
        public T peek() {
            return throttled ? null : super.peek();
        }
        
        @Override
        public boolean isEmpty() {
            return (peek() == null);
        }
    }

    protected class InboundActor extends AbstractActor<Message.Client> 
            implements FutureCallback<Object> {
    
        public InboundActor(Executor executor) {
            super(executor, 
                    ThrottlableMailbox.<Message.Client>newInstance(), 
                    newState());
    
            connection.register(this);
        }
        
        public void throttle(boolean throttled) {
            ((ThrottlableMailbox<?>) mailbox).throttle(throttled);
            if (! throttled) {
                schedule();
            }
        }
    
        @Subscribe
        public void handleTransitionEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                onFailure(new KeeperException.ConnectionLossException());
            }
        }

        @Override
        public void onSuccess(Object result) {  
            if (result instanceof FourLetterResponse) {
                outbound.send(result);
            } else if (result instanceof ConnectMessage.Response) {
                Session session = ((ConnectMessage.Response) result).toSession();
                if (session.initialized()) {
                    ServerConnectionExecutor.this.session = session;
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
        @Override
        public void send(Message.Client message) {
            super.send(message);
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
                Futures.addCallback(sessionExecutor.submit(SessionRequest.of(session.id(), request, request)), this);
            }
            return true;
        }
    
        @Override
        protected void doStop() {
            try {
                connection.unregister(this);
            } catch (IllegalArgumentException e) {}
            
            super.doStop();
        }
    }

    protected class OutboundActor extends PublisherActor 
        implements FutureCallback<Object> {
    
        public OutboundActor(Publisher publisher, Executor executor) {
            super(publisher, executor, AbstractActor.<Object>newQueue(), AbstractActor.newState());
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
                try {
                    Futures.addCallback(connection.write((Message.Server) input), this);
                } catch (Throwable t) {
                    onFailure(t);
                    return false;
                }
            } else if (Session.State.SESSION_EXPIRED == input) {
                onFailure(new KeeperException.SessionExpiredException());
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
                logger.debug("Closing connection {}", connection);
                connection.close();
                break;
            }
            default:
                break;
            }
        }
    }
}
