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
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Actor;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskMailbox;

public class ServerProtocolExecutor 
        implements Stateful<ProtocolState>, Eventful, Reference<ServerCodecConnection> {

    public static ServerProtocolExecutor newInstance(
            ServerCodecConnection codecConnection,
            ClientMessageExecutor anonymousExecutor,
            ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            ListeningExecutorService executor) {
        return new ServerProtocolExecutor(codecConnection, anonymousExecutor, sessionExecutors, executor);
    }
    
    public static class InboundMailbox extends ForwardingQueue<Message.ClientMessage> {

        public static InboundMailbox newInstance() {
            return new InboundMailbox(AbstractActor.<Message.ClientMessage>newQueue());
        }
        
        protected final Queue<Message.ClientMessage> delegate;
        protected volatile boolean throttled;
        
        protected InboundMailbox(Queue<Message.ClientMessage> delegate) {
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
        protected Queue<Message.ClientMessage> delegate() {
            return delegate;
        }
        
        @Override
        public Message.ClientMessage poll() {
            Message.ClientMessage message =  throttled ? null : super.poll();
            if (message instanceof OpCreateSession.Request) {
                throttled = true;
            }
            return message;
        }

        @Override
        public Message.ClientMessage peek() {
            return throttled ? null : super.peek();
        }
        
        @Override
        public boolean isEmpty() {
            return peek() != null;
        }
    }

    public class InboundActor extends AbstractActor<Message.ClientMessage, Void>
            implements FutureCallback<Message.ServerMessage> {

        protected final Connection<Message.ServerMessage> connection;
        protected final ClientMessageExecutor anonymousExecutor;
        protected final ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors;
        protected volatile SessionRequestExecutor sessionExecutor;

        protected InboundActor(
                Connection<Message.ServerMessage> connection,
                ClientMessageExecutor anonymousExecutor,
                ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
                Executor executor) {
            this(connection, anonymousExecutor, sessionExecutors, executor, 
                    InboundMailbox.newInstance(),
                    newState());
        }
        
        protected InboundActor(
                Connection<Message.ServerMessage> connection,
                ClientMessageExecutor anonymousExecutor,
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
        public synchronized void onSuccess(Message.ServerMessage result) {
            if (result instanceof OpCreateSession.Response.Valid) {
                Long sessionId = ((OpCreateSession.Response.Valid)result).asRecord().getSessionId();
                sessionExecutor = sessionExecutors.get(sessionId);
                sessionExecutor.register(submitted);
                throttle(false);
            } else {
                // if the response is Invalid, we want the response
                // to be flushed to the client before closing the connection
            }
        }

        @Override
        public boolean stop() {
            boolean stopped = super.stop();
            if (stopped) {
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}
                if (sessionExecutor != null) {
                    try {
                        sessionExecutor.unregister(submitted);
                    } catch (IllegalArgumentException e) {}
                }
            }
            return stopped;
        }
        
        @Override
        public void onFailure(Throwable t) {
            ServerProtocolExecutor.this.onFailure(t);
            stop();
        }
        
        @Subscribe
        public void handleConnectionState(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                onFailure(new KeeperException.ConnectionLossException());
            }
        }

        @Subscribe
        public void handleInbound(Message.ClientMessage message) throws InterruptedException {
            send(message);
        }

        @Override
        protected Void apply(Message.ClientMessage input) throws Exception {
            // ordering constraint: requests are submitted in the same
            // order that they are received
            ListenableFuture<? extends Message.ServerMessage> future;
            try {
                if (sessionExecutor != null) {
                    assert (input instanceof Operation.SessionRequest);
                    future = sessionExecutor.submit((Operation.SessionRequest)input);
                } else {
                    future = anonymousExecutor.submit(input);
                }
                submitted.send(future);
            } catch (Throwable t) {
                onFailure(t);
                return null;
            }
            if (input instanceof OpCreateSession.Request) {
                Futures.addCallback(future, this);
            }
            return null;
        }
    }

    public static class SubmittedMailbox extends TaskMailbox<Message.ServerMessage, ListenableFuture<? extends Message.ServerMessage>> {

        public static SubmittedMailbox newInstance() {
            return new SubmittedMailbox(AbstractActor.<ListenableFuture<? extends Message.ServerMessage>>newQueue());
        }
        
        protected SubmittedMailbox(Queue<ListenableFuture<? extends Message.ServerMessage>> delegate) {
            super(delegate);
        }

        @Override
        public synchronized ListenableFuture<? extends Message.ServerMessage> poll() {
            ListenableFuture<? extends Message.ServerMessage> next = peek();
            if (next != null) {
                return super.poll();
            } else {
                return null;
            }
        }

        @Override
        public ListenableFuture<? extends Message.ServerMessage> peek() {
            ListenableFuture<? extends Message.ServerMessage> next = super.peek();
            if ((next != null) && ! next.isDone()) {
                next = null;
            }
            return next;   
        }
        
        @Override
        public boolean isEmpty() {
            return peek() != null;
        }
    }
    
    public class SubmittedActor extends AbstractActor<ListenableFuture<? extends Message.ServerMessage>, Void>
            implements FutureCallback<Message.ServerMessage> {

        protected SubmittedActor(
                Executor executor) {
            this(executor, SubmittedMailbox.newInstance(), newState());
        }
        
        protected SubmittedActor(
                Executor executor,
                SubmittedMailbox mailbox,
                AtomicReference<Actor.State> state) {
            super(executor, mailbox, state);
        }

        @Override
        public void send(ListenableFuture<? extends ServerMessage> message) {
            super.send(message);
            Futures.addCallback(message, this);
        }
        
        @Subscribe
        @Override
        public void onSuccess(Message.ServerMessage result) {
            try {
                schedule();
                if (result instanceof Operation.SessionReply
                        && ((Operation.SessionReply)result).xid() == Records.OpCodeXid.NOTIFICATION.xid()) {
                    // we want to flush completed messages to outbound
                    // before queueing this notification
                    // to maintain the expected ordering
                    run();
                    outbound.send(result);
                } 
            } catch (Throwable t) {
                onFailure(t);
            }
        }
    
        @Override
        public void onFailure(Throwable t) {
            ServerProtocolExecutor.this.onFailure(t);
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
        
        @Override
        protected Void apply(ListenableFuture<? extends ServerMessage> input)
                throws Exception {         
            // ordering constraint: replies are queued for outbound in the order
            // that the requests were submitted       
            try {
                Message.ServerMessage reply = input.get();
                outbound.send(reply);
            } catch (Throwable t) {
                onFailure(t);
            }
            return null;
        }
    }
    
    public class OutboundActor extends AbstractActor<Message.ServerMessage, Void> 
            implements FutureCallback<Message.ServerMessage> {

        protected final Connection<Message.ServerMessage> connection;

        protected OutboundActor(
                Connection<Message.ServerMessage> connection,
                Executor executor) {
            this(connection, executor, 
                    AbstractActor.<Message.ServerMessage>newQueue(),
                    newState());
        }
        
        protected OutboundActor(
                Connection<Message.ServerMessage> connection,
                Executor executor,
                Queue<ServerMessage> mailbox,
                AtomicReference<Actor.State> state) {
            super(executor, mailbox, state);
            this.connection = connection;
        }

        @Override
        protected Void apply(Message.ServerMessage input) {
            // ordering constraint: messages are written in the order
            // that they were enqueued in outbound
            ListenableFuture<Message.ServerMessage> future;
            try {
                future = connection.write(input);
            } catch (Throwable t) {
                onFailure(t);
                return null;
            }
            Futures.addCallback(future, this);
            return null;
        }

        @Override
        public void onSuccess(Message.ServerMessage result) {
        }
    
        @Override
        public void onFailure(Throwable t) {
            ServerProtocolExecutor.this.onFailure(t);
            stop();
        }

        @Override
        public boolean stop() {
            boolean stopped = super.stop();
            if (stopped) {
                // TODO: flush?
                switch (get().state()) {
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
            return stopped;
        }
    }

    private final Logger logger;
    private final InboundActor inbound;
    private final SubmittedActor submitted;
    private final OutboundActor outbound;
    private final ServerCodecConnection connection;
    private final AtomicReference<Actor.State> state;
    
    private ServerProtocolExecutor(
            ServerCodecConnection connection,
            ClientMessageExecutor anonymousExecutor,
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
    public ServerCodecConnection get() {
        return connection;
    }

    @Override
    public ProtocolState state() {
        return get().codec().state();
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
