package edu.uw.zookeeper.protocol.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.OpSessionResult;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Automaton;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Stateful;
import edu.uw.zookeeper.util.TaskMailbox;
import edu.uw.zookeeper.util.TimeValue;

/**
 * Implemented for the case where submit is called by a different thread than
 * the Subscribe methods, but it is not safe for multiple threads to call 
 * submit or multiple threads to call handle*
 */
public class ClientProtocolExecutor
        implements ClientExecutor,
        Publisher,
        Stateful<ProtocolState>, 
        Reference<ClientCodecConnection> {
    
    public static ClientProtocolExecutorFactory factory(
            Factory<? extends Processor<Operation.Request, Operation.SessionRequest>> processorFactory,
            Factory<? extends ClientCodecConnection> connections,
            Reference<Long> lastZxid,
            TimeValue timeOut) {
        return ClientProtocolExecutorFactory.newInstance(
                processorFactory, connections, lastZxid, timeOut);
    }
    
    public static class ClientProtocolExecutorFactory implements DefaultsFactory<Factory<ConnectMessage.Request>, ClientProtocolExecutor> {

        public static ClientProtocolExecutorFactory newInstance(
                Factory<? extends Processor<Operation.Request, Operation.SessionRequest>> processorFactory,
                Factory<? extends ClientCodecConnection> connections,
                Reference<Long> lastZxid,
                TimeValue timeOut) {
            return newInstance(
                    processorFactory, connections, 
                    ConnectMessage.Request.NewRequest.factory(lastZxid, timeOut));
        }
        
        public static ClientProtocolExecutorFactory newInstance(
                Factory<? extends Processor<Operation.Request, Operation.SessionRequest>> processorFactory,
                Factory<? extends ClientCodecConnection> connections,
                Factory<ConnectMessage.Request> requests) {
            return new ClientProtocolExecutorFactory(
                    processorFactory, connections, requests); 
        }
        
        protected final Factory<? extends Processor<Operation.Request, Operation.SessionRequest>> processorFactory;
        protected final Factory<ConnectMessage.Request> requests;
        protected final Factory<? extends ClientCodecConnection> connections;
        
        protected ClientProtocolExecutorFactory(
                Factory<? extends Processor<Operation.Request, Operation.SessionRequest>> processorFactory,
                Factory<? extends ClientCodecConnection> connections,
                Factory<ConnectMessage.Request> requests) {
            this.processorFactory = processorFactory;
            this.connections = connections;
            this.requests = requests;
        }
        
        @Override
        public ClientProtocolExecutor get() {
            return get(requests);
        }

        @Override
        public ClientProtocolExecutor get(Factory<ConnectMessage.Request> requests) {
            return ClientProtocolExecutor.newInstance(
                    connections.get(), 
                    processorFactory.get(),
                    requests);
        }
    }
    
    public static ClientProtocolExecutor newInstance(
            ClientCodecConnection codecConnection,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            Factory<ConnectMessage.Request> requests) {
        return new ClientProtocolExecutor(
                codecConnection,
                processor,
                ClientProtocolInitializer.newInstance(codecConnection, requests));
    }
    
    public static class Pending implements Processor<PromiseTask<Operation.Request, Operation.SessionResult>, Void> {

        public static Pending newInstance(
                Processor<Operation.Request, Operation.SessionRequest> processor,
                Connection<Message.ClientSessionMessage> connection) {
            return new Pending(new LinkedBlockingQueue<PendingTask>(), processor, connection);
        }
        
        public class PendingTask 
                extends PromiseTask<Operation.SessionRequest, Operation.SessionResult>
                implements FutureCallback<Operation.SessionRequest> {

            protected PendingTask(
                    Operation.SessionRequest task,
                    Promise<Operation.SessionResult> delegate) {
                super(task, delegate);
            }
            
            @Override
            public void onSuccess(Operation.SessionRequest result) {
            }

            @Override
            public void onFailure(Throwable t) {
                setException(t);
            }
            
            @Override
            public boolean setException(Throwable t) {
                remove(this);
                return super.setException(t);
            }
        }

        // must be thread-safe
        protected final BlockingQueue<PendingTask> pending;
        protected final Processor<Operation.Request, Operation.SessionRequest> processor;
        protected final Connection<Message.ClientSessionMessage> connection;
        
        protected Pending(
                BlockingQueue<PendingTask> pending,
                Processor<Operation.Request, Operation.SessionRequest> processor,
                Connection<Message.ClientSessionMessage> connection) {
            this.pending = pending;
            this.processor = processor;
            this.connection = connection;
            
            connection.register(this);
        }
        
        @Override
        public Void apply(PromiseTask<Operation.Request, Operation.SessionResult> input) throws Exception {
            Operation.Request request = input.task();
            Operation.SessionRequest message;
            if (request instanceof Operation.SessionRequest) {
                message = (Operation.SessionRequest) request;
            } else {
                message = processor.apply(request);
            }
            
            // task needs to be in the queue before calling write
            PendingTask task = new PendingTask(message, input);
            pending.put(task);
            
            ListenableFuture<Operation.SessionRequest> future = connection.write(message);
            Futures.addCallback(future, task);

            return null;
        }
        
        @Subscribe
        public void handleSessionReply(Operation.SessionReply message) {
            PendingTask next = pending.peek();
            if (next != null) {
                if (message.xid() == next.task().xid()) {
                    remove(next);
                    next.set(OpSessionResult.of(next.task(), message));
                } else {
                    // assume it's an OpXid
                    // TODO
                }
            } else {
                // TODO: should this happen for non-opxid messages?
            }
        }

        @SuppressWarnings("unchecked")
        @Subscribe
        public void handleStateEvent(Automaton.Transition<?> event) {
            if (event.type().isAssignableFrom(Connection.State.class)) {
                handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
            }
        }
        
        public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
            if (Connection.State.CONNECTION_CLOSED == event.to()) {
                try {
                    connection.unregister(this);
                } catch (IllegalArgumentException e) {}

                Exception e = new KeeperException.ConnectionLossException();
                PendingTask task;
                while ((task = pending.poll()) != null) {
                    task.onFailure(e);
                }
            }
        }
        
        protected boolean remove(PendingTask task) {
            return pending.remove(task);
        }
    }

    private final ClientCodecConnection codecConnection;
    private final ClientProtocolInitializer initializer;
    private final Pending pending;
    private final TaskMailbox.ActorTaskExecutor<Operation.Request, Operation.SessionResult> outbound;
    
    private ClientProtocolExecutor(
            ClientCodecConnection codecConnection,
            Processor<Operation.Request, Operation.SessionRequest> processor,
            ClientProtocolInitializer initializer) {
        this.codecConnection = codecConnection;
        this.initializer = initializer;
        this.pending = Pending.newInstance(processor, codecConnection);
        this.outbound = TaskMailbox.executor(
                TaskMailbox.actor(pending, codecConnection));
                
        codecConnection.register(this);
    }
    
    @Override
    public ClientCodecConnection get() {
        return codecConnection;
    }
    
    @Override
    public ProtocolState state() {
        return get().codec().state();
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
    
    public ListenableFuture<Session> connect() {
        return initializer.call();
    }

    public ListenableFuture<Operation.SessionResult> disconnect() {
        return submit(Records.Requests.getInstance().get(OpCode.CLOSE_SESSION));
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request) {
        Promise<Operation.SessionResult> promise = outbound.newPromise();
        return submit(request, promise);
    }

    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        ProtocolState state = state();
        switch (state) {
        case ANONYMOUS:
            connect();
            break;
        case CONNECTING:
        case CONNECTED:
            break;
        default:
            throw new RejectedExecutionException(state.toString());
        }

        return outbound.submit(request, promise);
    }

    @SuppressWarnings("unchecked")
    @Subscribe
    public void handleStateEvent(Automaton.Transition<?> event) {
        if (event.type().isAssignableFrom(Connection.State.class)) {
            handleConnectionStateEvent((Automaton.Transition<Connection.State>)event);
        }
    }
    
    public void handleConnectionStateEvent(Automaton.Transition<Connection.State> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            try {
                unregister(this);
            } catch (IllegalArgumentException e) {}

            outbound.get().stop();
        }
    }

    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Override
    public void post(Object object) {
        get().post(object);
    }
}
