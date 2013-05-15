package edu.uw.zookeeper.protocol.client;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.net.ConnectionStateEvent;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.OpCreateSession;
import edu.uw.zookeeper.protocol.OpRecord;
import edu.uw.zookeeper.protocol.OpSessionResult;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskExecutor;
import edu.uw.zookeeper.util.TimeValue;

/**
 * Implemented for the case where submit is called by a different thread than
 * the Subscribe methods, but it is not safe for multiple threads to call 
 * submit or multiple threads to call handle*
 */
public class ClientProtocolConnection 
        implements Eventful, 
        Stateful<ProtocolState>, 
        Reference<ClientCodecConnection>, 
        TaskExecutor<Operation.Request, Operation.SessionResult> {
    
    public static interface RequestFuture extends ListenableFuture<Operation.SessionResult> {
        Operation.SessionRequest task();
    }
    
    public static ClientProtocolConnectionFactory factory(
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<? extends ClientCodecConnection> connections,
            Reference<Long> lastZxid,
            TimeValue timeOut) {
        return ClientProtocolConnectionFactory.newInstance(processor, connections, lastZxid, timeOut);
    }
    
    public static class ClientProtocolConnectionFactory implements DefaultsFactory<Factory<OpCreateSession.Request>, ClientProtocolConnection> {

        public static ClientProtocolConnectionFactory newInstance(
                Processor<Operation.Request, Operation.SessionRequest> processor,
                Factory<? extends ClientCodecConnection> connections,
                Reference<Long> lastZxid,
                TimeValue timeOut) {
            return newInstance(
                    processor, connections, 
                    OpCreateSession.Request.NewRequest.factory(lastZxid, timeOut));
        }
        
        public static ClientProtocolConnectionFactory newInstance(
                Processor<Operation.Request, Operation.SessionRequest> processor,
                Factory<? extends ClientCodecConnection> connections,
                Factory<OpCreateSession.Request> requests) {
            return new ClientProtocolConnectionFactory(
                    processor, connections, requests); 
        }
        
        protected final Processor<Operation.Request, Operation.SessionRequest> processor;
        protected final Factory<OpCreateSession.Request> requests;
        protected final Factory<? extends ClientCodecConnection> connections;
        
        protected ClientProtocolConnectionFactory(
                Processor<Operation.Request, Operation.SessionRequest> processor,
                Factory<? extends ClientCodecConnection> connections,
                Factory<OpCreateSession.Request> requests) {
            this.processor = processor;
            this.connections = connections;
            this.requests = requests;
        }
        
        @Override
        public ClientProtocolConnection get() {
            return get(requests);
        }

        @Override
        public ClientProtocolConnection get(Factory<OpCreateSession.Request> requests) {
            ClientCodecConnection codecConnection = connections.get();
            return ClientProtocolConnection.newInstance(
                    codecConnection, 
                    processor,
                    requests);
        }
    }
    
    public static ClientProtocolConnection newInstance(
            ClientCodecConnection codecConnection,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<OpCreateSession.Request> requests) {
        return new ClientProtocolConnection(
                codecConnection,
                processor,
                ClientProtocolInitializer.newInstance(codecConnection, requests));
    }
    
    public static class RequestPromise 
            extends PromiseTask<Operation.SessionRequest, Operation.SessionResult>
            implements RequestFuture, FutureCallback<Operation.SessionReply> {

        public static RequestPromise of(
                Operation.SessionRequest request) {
            return of(request, SettableFuturePromise.<Operation.SessionResult>create());
        }

        public static RequestPromise of(
                Operation.SessionRequest request, 
                Promise<Operation.SessionResult> delegate) {
            return new RequestPromise(request, delegate);
        }
        
        protected RequestPromise(
                Operation.SessionRequest task,
                Promise<Operation.SessionResult> delegate) {
            super(task, delegate);
        }

        @Override
        public void onSuccess(Operation.SessionReply result) {
            set(OpSessionResult.of(task(), result));
        }

        @Override
        public void onFailure(Throwable t) {
            setException(t);
        }
    }

    // must be thread-safe
    private final Queue<RequestPromise> pending;
    private final ClientCodecConnection codecConnection;
    private final ClientProtocolInitializer initializer;
    private final Processor<Operation.Request, Operation.SessionRequest> processor;
    
    private ClientProtocolConnection(
            ClientCodecConnection codecConnection,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ClientProtocolInitializer initializer) {
        this.codecConnection = codecConnection;
        this.initializer = initializer;
        this.processor = processor;
        this.pending = new LinkedBlockingQueue<RequestPromise>();
        
        register(this);
    }
    
    @Override
    public ClientCodecConnection get() {
        return codecConnection;
    }
    
    @Override
    public ProtocolState state() {
        return get().asCodec().state();
    }

    public Session session() {
        if (initializer.isDone()) {
            try {
                return initializer.get();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
        } else {
            return Session.uninitialized();
        }
    }
    
    public ListenableFuture<Session> connect() throws IOException {
        return initializer.call();
    }

    public RequestFuture disconnect() {
        return submit(OpRecord.OpRequest.newInstance(OpCode.CLOSE_SESSION));
    }

    public void cancel() {
        try {
            unregister(this);
        } catch (IllegalArgumentException e) {}

        RequestPromise task = pending.poll();
        while (task != null) {
            task.cancel(false);
            task = pending.poll();
        }
    }
    
    /**
     * Don't call concurrently!
     * 
     * @throws RejectedExecutionException
     */
    @Override
    public RequestFuture submit(Operation.Request request) {
        ProtocolState state = state();
        switch (state) {
        case ANONYMOUS:
            try {
                connect();
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

        Operation.SessionRequest message;
        if (request instanceof Operation.SessionRequest) {
            message = (Operation.SessionRequest) request;
        } else {
            try {
                message = processor.apply(request);
            } catch (Exception e) {
                throw new RejectedExecutionException(e);
            }
        }
        
        // TODO: what about opxid requests?
        // TODO: this part of the code needs to be atomic
        // so that the order in which messages are encoded and sent is the same
        // as the order in the queue
        // but we can't just use a a simple lock because
        // it's possible that write() could cause an event
        // that would hook into unknown upcalls
        RequestPromise task = RequestPromise.of(message);
        pending.add(task);
        try {
            codecConnection.write(message);
        } catch (Exception e) {
            task.cancel(false);
            pending.remove(task);
            if (e instanceof IOException) {
                throw new RejectedExecutionException(e);
            }
            throw Throwables.propagate(e);
        }
        return task;
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
            RequestPromise task = pending.poll();
            while (task != null) {
                task.onFailure(e);
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
        RequestPromise next = pending.peek();
        if (next != null) {
            if (message.xid() == next.task().xid()) {
                pending.poll();
                next.onSuccess(message);
            } else {
                // TODO
            }
        } else {
            // TODO: should this happen for non-opxid messages?
        }
    }
}
