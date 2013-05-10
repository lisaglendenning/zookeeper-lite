package edu.uw.zookeeper.protocol.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolConnection implements Stateful<ProtocolState>, FutureCallback<Message.ServerMessage>, Eventful, Callable<Void> {

    public static ServerProtocolConnection newInstance(
            ServerCodecConnection codecConnection,
            ClientMessageExecutor anonymousExecutor,
            ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            ListeningExecutorService executor) {
        return new ServerProtocolConnection(codecConnection, anonymousExecutor, sessionExecutors, executor);
    }
    
    public static enum State {
        WAITING, SCHEDULED, TERMINATED;
    }

    private final Logger logger = LoggerFactory
            .getLogger(ServerProtocolConnection.class);
    private final BlockingQueue<Message.ClientMessage> inbound;
    private final BlockingQueue<ListenableFuture<? extends Message.ServerMessage>> submitted;
    private final BlockingQueue<Message.ServerMessage> outbound;
    private final ClientMessageExecutor anonymousExecutor;
    private final ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors;
    private final ListeningExecutorService executor;
    private final ServerCodecConnection codecConnection;
    private final AtomicReference<State> state;
    private volatile SessionRequestExecutor sessionExecutor;
    private volatile boolean throttled;
    
    private ServerProtocolConnection(
            ServerCodecConnection codecConnection,
            ClientMessageExecutor anonymousExecutor,
            ParameterizedFactory<Long, ? extends SessionRequestExecutor> sessionExecutors,
            ListeningExecutorService executor) {
        this.codecConnection = codecConnection;
        this.anonymousExecutor = anonymousExecutor;
        this.sessionExecutors = sessionExecutors;
        this.executor = executor;
        this.inbound = new LinkedBlockingQueue<Message.ClientMessage>();
        this.submitted = new LinkedBlockingQueue<ListenableFuture<? extends Message.ServerMessage>>();
        this.outbound = new LinkedBlockingQueue<Message.ServerMessage>();
        this.state = new AtomicReference<State>(State.WAITING);
        this.throttled = false;
        this.sessionExecutor = null;
        register(this);
    }
    
    public ServerCodecConnection asCodecConnection() {
        return codecConnection;
    }

    @Override
    public ProtocolState state() {
        return asCodecConnection().asCodec().state();
    }
    
    @Subscribe
    public void handleConnectionState(ConnectionStateEvent event) {
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            onFailure(new KeeperException.ConnectionLossException());
            break;
        default:
            break;
        }
    }

    /**
     * Don't call concurrently.
     */
    @Subscribe
    public void handleInbound(Message.ClientMessage message) throws InterruptedException {
        if (state.get() == State.TERMINATED) {
            logger.debug("Dropping {}", message);
        } else {
            inbound.put(message);
            schedule();
        }
    }

    /**
     * Thread-safe
     */
    public Void call() {
        if (! state.compareAndSet(State.SCHEDULED, State.WAITING)) {
            return null;
        }
        
        try {
            while (!throttled && !inbound.isEmpty()) {
                submitInbound();
            }
        } catch (Exception e) {
            onFailure(e);
            return null;
        }
        
        return null;
    }
    
    private void schedule() {
        if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
            executor.submit(this);
        }
    }
    
    private synchronized void submitInbound() {
        // ordering constraint: requests are submitted in the same
        // order that they are received
        Message.ClientMessage message = inbound.peek();
        if (message != null) {
            if (message instanceof OpCreateSession.Request) {
                throttled = true;
            }
            ListenableFuture<? extends Message.ServerMessage> future;
            if (sessionExecutor != null) {
                assert (message instanceof Operation.SessionRequest);
                future = sessionExecutor.submit((Operation.SessionRequest)message);
            } else {
                future = anonymousExecutor.submit(message);
            }
            inbound.remove(message);
            submitted.add(future);
            Futures.addCallback(future, this);
        }
    }
    
    private synchronized boolean checkSubmitted() {
        // check for completed requests
        // ordering constraint: replies are queued for outbound in the order
        // that the requests were submitted
        boolean changed = false;
        ListenableFuture<? extends Message.ServerMessage> future = submitted.peek();
        while (future != null && future.isDone()) {
            Message.ServerMessage reply;
            try {
                reply = future.get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            changed = true;
            submitted.remove(future);
            outbound.add(reply);
            future = submitted.peek();
        }
        return changed;
    }
    
    private synchronized void flush() {
        // write outbound messages
        // ordering constraint: messages are written in the order
        // that they were enqueued in outbound
        Message.ServerMessage reply = outbound.peek();
        while (reply != null) {
            try {
                codecConnection.write(reply);
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            outbound.remove(reply);
            reply = outbound.peek();
        }
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
    @Override
    public synchronized void onSuccess(Message.ServerMessage result) {
        try {        
            if (state.get() == State.TERMINATED) {
                logger.debug("Dropping {}", result);
                return;
            }
            if (result instanceof Operation.SessionReply
                    && ((Operation.SessionReply)result).xid() == Records.OpCodeXid.NOTIFICATION.xid()) {
                checkSubmitted();
                outbound.add(result);
                flush();
            } else {
                if (result instanceof OpCreateSession.Response.Valid) {
                    Long sessionId = ((OpCreateSession.Response.Valid)result).asRecord().getSessionId();
                    sessionExecutor = sessionExecutors.get(sessionId);
                    sessionExecutor.register(this);
                    throttled = false;
                    schedule();
                }
                // assuming that the corresponding future isDone when this callback is called
                if (checkSubmitted()) {
                    flush();
                }
            }
        } catch (Exception e) {
            onFailure(e);
        }
    }

    /**
     * Thread-safe
     */
    @Override
    public void onFailure(Throwable t) {
        if (state.getAndSet(State.TERMINATED) == State.TERMINATED) {
            return;
        }

        try {
            unregister(this);
        } catch (IllegalArgumentException e) {}
        
        Connection connection = codecConnection.asConnection();
        switch (connection.state()) {
        case CONNECTION_OPENING:
        case CONNECTION_OPENED:
            logger.debug("Closing connection", t);
            connection.close();
            break;
        default:
            break;
        }
    }

    @Override
    public void register(Object handler) {
        codecConnection.register(handler);
        codecConnection.asConnection().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        codecConnection.unregister(handler);
        try {
            codecConnection.asConnection().unregister(handler);
        } catch (IllegalArgumentException e) {}
    }
}
