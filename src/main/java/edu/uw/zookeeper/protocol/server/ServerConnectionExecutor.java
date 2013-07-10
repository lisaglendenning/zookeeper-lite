package edu.uw.zookeeper.protocol.server;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ForwardingQueue;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import edu.uw.zookeeper.ServerMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Actor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.FutureQueue;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.TaskExecutor;

public class ServerConnectionExecutor<C extends Connection<Message.Server>>
        implements Eventful, Reference<C> {

    public static <C extends Connection<Message.Server>> ServerConnectionExecutor<C> newInstance(
            C connection,
            ServerMessageExecutor anonymousExecutor,
            ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            Executor executor) {
        return new ServerConnectionExecutor<C>(
                connection, anonymousExecutor, sessionExecutors, executor);
    }
    
    public static class InboundMailbox extends ForwardingQueue<Message.Client> {

        public static InboundMailbox newInstance() {
            return new InboundMailbox(AbstractActor.<Message.Client>newQueue());
        }
        
        protected final Queue<Message.Client> delegate;
        protected volatile boolean throttled;
        
        protected InboundMailbox(Queue<Message.Client> delegate) {
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
        protected Queue<Message.Client> delegate() {
            return delegate;
        }
        
        @Override
        public Message.Client poll() {
            Message.Client message =  throttled ? null : super.poll();
            if (message instanceof ConnectMessage.Request) {
                throttled = true;
            }
            return message;
        }

        @Override
        public Message.Client peek() {
            return throttled ? null : super.peek();
        }
        
        @Override
        public boolean isEmpty() {
            return (peek() == null);
        }
    }

    public class InboundActor extends AbstractActor<Message.Client>
            implements FutureCallback<Message.Server> {

        protected final Connection<Message.Server> connection;
        protected final TaskExecutor<Message.Client, Message.Server> anonymousExecutor;
        protected final ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors;
        protected volatile SessionRequestExecutor sessionExecutor;

        protected InboundActor(
                Connection<Message.Server> connection,
                ServerMessageExecutor anonymousExecutor,
                ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
                Executor executor) {
            this(connection, anonymousExecutor, sessionExecutors, executor, 
                    InboundMailbox.newInstance(),
                    newState());
        }
        
        protected InboundActor(
                Connection<Message.Server> connection,
                ServerMessageExecutor anonymousExecutor,
                ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
                Executor executor, 
                InboundMailbox mailbox,
                AtomicReference<Actor.State> state) {
            super(executor, mailbox, state);
            this.connection = connection;
            this.anonymousExecutor = anonymousExecutor;
            this.sessionExecutors = sessionExecutors;
            this.sessionExecutor = null;
            
            connection.register(this);
        }

        public void throttle(boolean throttled) {
            ((InboundMailbox) mailbox).throttle(throttled);
            if (! throttled) {
                schedule();
            }
        }

        @Override
        public synchronized void onSuccess(Message.Server result) {
            if (result instanceof ConnectMessage.Response.Valid) {
                Long sessionId = ((ConnectMessage.Response.Valid)result).getSessionId();
                sessionExecutor = sessionExecutors.get(sessionId);
                sessionExecutor.register(submitted);
                throttle(false);
            } else {
                // if the response is Invalid, we want the response
                // to be flushed to the client before closing the connection
            }
        }

        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.onFailure(t);
            stop();
        }

        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                onFailure(new KeeperException.ConnectionLossException());
            }
        }

        @Subscribe
        @Override
        public void send(Message.Client message) {
            super.send(message);
        }

        @Override
        protected boolean apply(Message.Client input) throws Exception {
            // ordering constraint: requests are submitted in the same
            // order that they are received
            ListenableFuture<? extends Message.Server> future;
            try {
                if (sessionExecutor != null) {
                    future = sessionExecutor.submit((Message.ClientRequest) input);
                } else {
                    future = anonymousExecutor.submit(input);
                }
                submitted.send(future);
            } catch (Throwable t) {
                onFailure(t);
                return false;
            }
            if (input instanceof ConnectMessage.Request) {
                Futures.addCallback(future, this);
            }
            return true;
        }

        @Override
        protected void doStop() {
            super.doStop();
            
            try {
                connection.unregister(this);
            } catch (IllegalArgumentException e) {}
            if (sessionExecutor != null) {
                try {
                    sessionExecutor.unregister(submitted);
                } catch (IllegalArgumentException e) {}
            }
        }
    }

    public class SubmittedActor extends AbstractActor<ListenableFuture<? extends Message.Server>>
            implements FutureCallback<Message.Server> {

        protected SubmittedActor(
                Executor executor) {
            super(executor, FutureQueue.<ListenableFuture<? extends Message.Server>>create(), newState());
        }

        @Override
        public void send(ListenableFuture<? extends Message.Server> message) {
            super.send(message);
            Futures.addCallback(message, this);
        }
        
        @Override
        public void onSuccess(Message.Server result) {
            schedule();
        }
    
        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.onFailure(t);
            stop();
        }
        
        @Subscribe
        public void handleSessionEvent(Session.State event) {
            switch (event) {
            case SESSION_EXPIRED:
                // TODO: try to flush messages first?
                onFailure(new KeeperException.SessionExpiredException());
                break;
            default:
                break;
            }
        }
        
        @Subscribe
        public void handleSessionReply(Message.ServerResponse message) {
            if (OpCodeXid.NOTIFICATION.xid() == message.xid()) {
                // we need to flush completed messages to outbound
                // before queueing this message
                // to maintain the expected ordering
                try {
                    flush(message);
                } catch (Throwable t) {
                    onFailure(t);
                }
            }
        }
        
        protected synchronized void flush(Message.Server message) throws Exception {
            doRun();
            outbound.send(message);
        }
        
        @Override
        protected synchronized void doRun() throws Exception {
            super.doRun();
        }
        
        @Override
        protected boolean apply(ListenableFuture<? extends Message.Server> input)
                throws Exception {         
            // ordering constraint: replies are queued for outbound in the order
            // that the requests were submitted       
            try {
                Message.Server reply = input.get();
                outbound.send(reply);
                return true;
            } catch (Throwable t) {
                onFailure(t);
                return false;
            }
        }
    }
    
    public class OutboundActor extends AbstractActor<Message.Server> 
            implements FutureCallback<Message.Server> {

        protected final Connection<Message.Server> connection;

        protected OutboundActor(
                Connection<Message.Server> connection,
                Executor executor) {
            super(executor, AbstractActor.<Message.Server>newQueue(), AbstractActor.newState());
            this.connection = connection;
        }

        @Override
        public void onSuccess(Message.Server result) {
        }
    
        @Override
        public void onFailure(Throwable t) {
            ServerConnectionExecutor.this.onFailure(t);
            stop();
        }

        @Override
        protected boolean apply(Message.Server input) {
            // ordering constraint: messages are written in the order
            // that they were enqueued in outbound
            try {
                ListenableFuture<Message.Server> future = connection.write(input);
                Futures.addCallback(future, this);
                return true;
            } catch (Throwable t) {
                onFailure(t);
                return false;
            }
        }

        @Override
        protected void doStop() {
            super.doStop();
            
            // TODO: flush?
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

    private final Logger logger;
    private final InboundActor inbound;
    private final SubmittedActor submitted;
    private final OutboundActor outbound;
    private final C connection;
    private final AtomicReference<Actor.State> state;
    
    private ServerConnectionExecutor(
            C connection,
            ServerMessageExecutor anonymousExecutor,
            ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            Executor executor) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.connection = connection;
        this.inbound = new InboundActor(connection, anonymousExecutor, sessionExecutors, executor);
        this.submitted = new SubmittedActor(executor);
        this.outbound = new OutboundActor(connection, executor);
        this.state = AbstractActor.newState();
    }
    
    @Override
    public C get() {
        return connection;
    }

    public void onFailure(Throwable t) {
        if (state.getAndSet(Actor.State.TERMINATED) == Actor.State.TERMINATED) {
            return;
        }

        logger.debug("Stopping", t);
        
        Actor<?>[] actors = { inbound, submitted, outbound };
        for (Actor<?> actor: actors) {
            actor.stop();
        }
    }

    @Override
    public void register(Object handler) {
        get().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        get().unregister(handler);
    }
}
