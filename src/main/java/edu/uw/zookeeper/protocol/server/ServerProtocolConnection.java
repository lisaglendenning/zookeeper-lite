package edu.uw.zookeeper.protocol.server;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.event.ConnectionStateEvent;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Stateful;

public class ServerProtocolConnection implements Stateful<ProtocolState>, FutureCallback<Message.ServerMessage>, Eventful, Callable<Void> {

    public static ServerProtocolConnection newInstance(
            ServerCodecConnection codecConnection,
            ServerExecutor callback,
            ListeningExecutorService executor) {
        return new ServerProtocolConnection(codecConnection, callback, executor);
    }
    
    public static class RequestTask extends AbstractPair<Message.ClientMessage, ListenableFuture<Message.ServerMessage>> {

        public static RequestTask newInstance(ClientMessage first,
                ListenableFuture<ServerMessage> second) {
            return new RequestTask(first, second);
        }
        
        protected RequestTask(ClientMessage first,
                ListenableFuture<ServerMessage> second) {
            super(first, second);
        }
        
        public Message.ClientMessage request() {
            return first;
        }
        
        public ListenableFuture<Message.ServerMessage> future() {
            return second;
        }
    }
    
    public static enum State {
        WAITING, SCHEDULED, TERMINATED;
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ServerProtocolConnection.class);
    // must be thread-safe
    private final Queue<Message.ClientMessage> received;
    private final Queue<RequestTask> submitted;
    private final ServerExecutor callback;
    private final ListeningExecutorService executor;
    private final ServerCodecConnection codecConnection;
    private final AtomicReference<State> state;
    private volatile boolean throttled;
    private volatile boolean pendingChanges;
    
    private ServerProtocolConnection(
            ServerCodecConnection codecConnection,
            ServerExecutor callback,
            ListeningExecutorService executor) {
        this.codecConnection = codecConnection;
        this.callback = callback;
        this.executor = executor;
        this.received = new LinkedBlockingQueue<Message.ClientMessage>();
        this.submitted = new LinkedBlockingQueue<RequestTask>();
        this.state = new AtomicReference<State>(State.WAITING);
        this.throttled = false;
        this.pendingChanges = false;
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

    @Subscribe
    public void handleRequest(Message.ClientMessage message) {
        if (state.get() == State.TERMINATED) {
            logger.debug("Dropping {}", message);
        } else {
            received.add(message);
            pendingChanges = true;
            schedule();
        }
    }

    /**
     * Don't call concurrently!
     */
    public Void call() {
        if (state.get() != State.SCHEDULED) {
            return null;
        }
        
        pendingChanges = false;
        
        // submit received messages unless throttled
        while (!throttled && received.peek() != null) {
            Message.ClientMessage message = received.poll();
            if (message instanceof OpCreateSession.Request) {
                throttled = true;
            }
            ListenableFuture<Message.ServerMessage> future = callback.submit(message);
            RequestTask task = RequestTask.newInstance(message, future);
            submitted.add(task);
            Futures.addCallback(future, this);
        }

        // write responses for completed requests
        while (submitted.peek() != null && submitted.peek().future().isDone()) {
            RequestTask task = submitted.poll();
            Message.ServerMessage result;
            try {
                result = task.future().get();
            } catch (Exception e) {
                onFailure(e);
                return null;
            }
            try {
                codecConnection.write(result);
            } catch (Exception e) {
                logger.debug("Exception while writing {}", result, e);
                onFailure(e);
                return null;
            }
        }
        
        if (state.compareAndSet(State.SCHEDULED, State.WAITING) && pendingChanges) {
            schedule();
        }
        return null;
    }
    
    protected void schedule() {
        if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
           executor.submit(this);
        }
    }

    /**
     * Thread-safe
     */
    @Override
    public void onSuccess(Message.ServerMessage result) {
        if (state.get() == State.TERMINATED) {
            logger.debug("Dropping {}", result);
        } else {
            if (result instanceof OpCreateSession.Response.Valid) {
                throttled = false;
            }
            pendingChanges = true;
            schedule();
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
