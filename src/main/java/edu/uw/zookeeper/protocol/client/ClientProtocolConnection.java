package edu.uw.zookeeper.protocol.client;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.net.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.SettableTask;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TimeValue;

/**
 * Implemented for the case where submit is called by a different thread than
 * the Subscribe methods, but it is not safe for multiple threads to call 
 * submit or multiple threads to call handle*
 */
public class ClientProtocolConnection implements Eventful, SessionRequestExecutor, Stateful<ProtocolState> {

    public static ClientProtocolConnection newSession(
            ClientCodecConnection codecConnection,
            Reference<Long> lastZxid,
            TimeValue timeOut) {
        ClientProtocolInitializer initializer = ClientProtocolInitializer.newSession(codecConnection, lastZxid, timeOut);
        return newInstance(codecConnection, initializer);
    }

    public static ClientProtocolConnection renewSession(
            ClientCodecConnection codecConnection,
            Reference<Long> lastZxid,
            Session session) {
        ClientProtocolInitializer initializer = ClientProtocolInitializer.renewSession(codecConnection, lastZxid, session);
        return newInstance(codecConnection, initializer);
    }
    
    public static ClientProtocolConnection newInstance(
            ClientCodecConnection codecConnection,
            ClientProtocolInitializer initializer) {
        return new ClientProtocolConnection(
                codecConnection,
                initializer);
    }
    
    public static class RequestTask extends SettableTask<Operation.SessionRequest, Operation.SessionReply> {
        public static RequestTask create(Operation.SessionRequest request) {
            return new RequestTask(request);
        }

        protected RequestTask(Operation.SessionRequest request) {
            super(request);
        }
    }

    // must be thread-safe
    private final Queue<RequestTask> pending;
    private final ClientCodecConnection codecConnection;
    private final ClientProtocolInitializer initializer;
    
    private ClientProtocolConnection(
            ClientCodecConnection codecConnection,
            ClientProtocolInitializer initializer) {
        this.codecConnection = codecConnection;
        this.initializer = initializer;
        this.pending = new LinkedBlockingQueue<RequestTask>();
        register(this);
    }
    
    public ClientCodecConnection asCodecConnection() {
        return codecConnection;
    }
    
    @Override
    public ProtocolState state() {
        return asCodecConnection().asCodec().state();
    }
    
    public ListenableFuture<Session> connect() throws IOException {
        ProtocolState state = state();
        switch (state) {
        case ANONYMOUS:
            break;
        default:
            throw new IllegalStateException(state.toString());
        }
        return initializer.call();
    }
    
    public void cancel() {
        try {
            unregister(this);
        } catch (IllegalArgumentException e) {}

        RequestTask task = pending.poll();
        while (task != null) {
            task.future().cancel(false);
            task = pending.poll();
        }
    }
    
    /**
     * Don't call concurrently!
     * 
     * @throws RejectedExecutionException
     */
    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        ProtocolState state = state();
        switch (state) {
        case ANONYMOUS:
            try {
                initializer.call();
            } catch (IOException e) {
                throw new RejectedExecutionException(e);
            }
            break;
        case CONNECTING:
        case CONNECTED:
            break;
        default:
            throw new IllegalStateException(state.toString());
        }

        // TODO: what about opxid requests?
        // TODO: this part of the code needs to be atomic
        // so that the order in which messages are encoded and sent is the same
        // as the order in the queue
        // but we can't just use a a simple lock because
        // it's possible that write() could cause an event
        // that would hook into unknown upcalls
        RequestTask task = RequestTask.create(request);
        pending.add(task);
        try {
            codecConnection.write(request);
        } catch (Exception e) {
            task.future().cancel(false);
            pending.remove(task);
            if (e instanceof IOException) {
                throw new RejectedExecutionException(e);
            }
            throw Throwables.propagate(e);
        }
        return task.future();
    }
    
    @Override
    public void register(Object handler) {
        // According to EventBus source code, registering the same
        // object multiple times is a no-op
        // so...let's register for everything!
        codecConnection.register(handler);
        codecConnection.asConnection().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        codecConnection.unregister(handler);
        try {
            codecConnection.asConnection().unregister(handler);
        } catch (IllegalArgumentException e) {
            // by doing this multiple registration,
            // we could easily attempt to unregister multiple times
            // if the same underlying Publisher is used
        }
    }
    
    @Subscribe
    public void handleConnectionState(ConnectionStateEvent event) {
        switch (event.event().to()) {
        case CONNECTION_CLOSED:
            try {
                unregister(this);
            } catch (IllegalArgumentException e) {}

            Exception e = new KeeperException.ConnectionLossException();
            RequestTask task = pending.poll();
            while (task != null) {
                task.future().setException(e);
                task = pending.poll();
            }
            break;
        default:
            break;
        }
    }
    
    /**
     * Don't call concurrently!
     */
    @Subscribe
    public void handleReply(Operation.SessionReply message) {
        // peek and poll need to be atomic!
        RequestTask next = pending.peek();
        if (next != null) {
            if (message.xid() == next.task().xid()) {
                pending.poll();
                next.future().set(message);
            } else {
                // TODO
            }
        } else {
            // TODO: should this happen for non-opxid messages?
        }
    }
}
