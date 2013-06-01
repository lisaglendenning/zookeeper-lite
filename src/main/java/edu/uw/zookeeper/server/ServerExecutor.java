package edu.uw.zookeeper.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.Processors.*;

public class ServerExecutor implements ClientMessageExecutor, Executor, Callable<Object>, ParameterizedFactory<Long, ServerExecutor.PublishingSessionRequestExecutor> {

    public interface PublishingSessionRequestExecutor extends SessionRequestExecutor, Publisher {}
    
    public static ServerExecutor newInstance(
            final ListeningExecutorService executor,
            final Factory<Publisher> publisherFactory,
            final ExpiringSessionManager sessions) {
        AssignZxidProcessor zxids = AssignZxidProcessor.newInstance();
        return newInstance(executor, publisherFactory, sessions, zxids);
    }
    
    public static ServerExecutor newInstance(
            final ListeningExecutorService executor,
            final Factory<Publisher> publisherFactory,
            final ExpiringSessionManager sessions,
            AssignZxidProcessor zxids) {
        return new ServerExecutor(executor, publisherFactory, sessions, zxids);
    }

    public static enum State {
        WAITING, SCHEDULED, TERMINATED;
    }

    public static class ServerClientMessageExecutor implements ClientMessageExecutor {
        
        public static ServerClientMessageExecutor newInstance(
                Reference<Long> zxids,
                SessionTable sessions) {
            return newInstance(processor(zxids, sessions));
        }
        
        public static Processor<Message.ClientMessage, Message.ServerMessage> processor(
                Reference<Long> zxids,
                SessionTable sessions) {
            FilteringProcessor<Message.ClientMessage, Message.ServerMessage> createProcessor =
                    OpCreateSessionProcessor.filtered(sessions, zxids);
            FilteringProcessor<Message.ClientMessage, Message.ServerMessage> errorProcessor =
                    new FilteredProcessor<Message.ClientMessage, Message.ServerMessage>(
                            Predicates.alwaysTrue(),
                            new ErrorProcessor<Message.ClientMessage, Message.ServerMessage>());
            @SuppressWarnings("unchecked")
            Processor<Message.ClientMessage, Message.ServerMessage> processor = 
                    FilteredProcessors.newInstance(createProcessor, errorProcessor);
            return processor;
        }

        public static ServerClientMessageExecutor newInstance(
                Processor<Message.ClientMessage, Message.ServerMessage> processor) {
            return new ServerClientMessageExecutor(processor);
        }
        
        public static class ClientMessageTask extends ProcessorThunk<Message.ClientMessage, Message.ServerMessage> {
            public static ClientMessageTask newInstance(
                    Processor<? super Message.ClientMessage, ? extends Message.ServerMessage> first,
                    Message.ClientMessage second) {
                return new ClientMessageTask(first, second);
            }
            
            public ClientMessageTask(
                    Processor<? super Message.ClientMessage, ? extends Message.ServerMessage> first,
                    Message.ClientMessage second) {
                super(first, second);
            }}
        
        protected final Processor<Message.ClientMessage, Message.ServerMessage> processor;
        
        protected ServerClientMessageExecutor(
                Processor<Message.ClientMessage, Message.ServerMessage> processor) {
            this.processor = processor;
        }
        
        @Override
        public ListenableFutureTask<Message.ServerMessage> submit(Message.ClientMessage request) {
            return submit(request, PromiseTask.<Message.ServerMessage>newPromise());
        }

        @Override
        public ListenableFutureTask<Message.ServerMessage> submit(Message.ClientMessage request,
                Promise<Message.ServerMessage> promise) {
            return ListenableFutureTask.create(ClientMessageTask.newInstance(processor, request));
        }   
    }
    
    protected final ListeningExecutorService executor;
    protected final Factory<Publisher> publisherFactory;
    protected final AssignZxidProcessor zxids;
    protected final ExpiringSessionManager sessions;
    protected final BlockingQueue<Runnable> pending;
    protected final ConcurrentMap<Long, PublishingSessionRequestExecutor> executors;
    protected final ServerClientMessageExecutor anonymousExecutor;
    protected final AtomicReference<State> state;
    
    protected ServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids) {
        this.executor = executor;
        this.publisherFactory = publisherFactory;
        this.zxids = zxids;
        this.sessions = sessions;
        this.pending = new LinkedBlockingQueue<Runnable>();
        this.executors = Maps.newConcurrentMap();
        this.anonymousExecutor = ServerClientMessageExecutor.newInstance(zxids, sessions);
        this.state = new AtomicReference<State>(State.WAITING);
        
        sessions.register(this);
    }
    
    public ListeningExecutorService executor() {
        return executor;
    }
    
    public ExpiringSessionManager sessions() {
        return sessions;
    }
    
    public AssignZxidProcessor zxids() {
        return zxids;
    }

    @Override
    public void execute(Runnable task) {
        try {
            pending.put(task);
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
        schedule();
    }
    
    @Override
    public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request) {
        return submit(request, PromiseTask.<Message.ServerMessage>newPromise());
    }
    
    @Override
    public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request,
            Promise<Message.ServerMessage> promise) {
        ListenableFutureTask<Message.ServerMessage> task = anonymousExecutor.submit(request);
        execute(task);
        return task;
    }

    private void schedule() {
        if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
            executor.submit(this);
        }
    }

    /**
     * This is where global ordering is imposed.
     * 
     * Where zxid should be assigned, and where externally-observable changes should be serialized.
     */
    @Override
    public Void call() {
        while (! pending.isEmpty()) {
            next();
        }
        
        state.compareAndSet(State.SCHEDULED, State.WAITING);
        
        return null;
    }
    
    protected synchronized void next() {
        Runnable next = pending.peek();
        if (next != null) {
            try {
                next.run();
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            pending.remove(next);
        }        
    }

    @Override
    public PublishingSessionRequestExecutor get(Long sessionId) {
        PublishingSessionRequestExecutor executor = executors.get(sessionId);
        if (executor == null) {
            executor = newSessionRequestExecutor(sessionId);
            if (executors.putIfAbsent(sessionId, executor) != null) {
                throw new AssertionError();
            }
        }
        return executor;
    }
    
    @Subscribe
    public void handleSessionStateEvent(SessionStateEvent event) {
        Long sessionId = event.session().id();
        PublishingSessionRequestExecutor executor = executors.get(sessionId);
        if (executor != null) {
            Session.State state = event.event();
            executor.post(state);
            switch(state) {
            case SESSION_CLOSED:
                executors.remove(sessionId, executor);
                break;
            default:
                break;
            }
        }
    }
    
    protected PublishingSessionRequestExecutor newSessionRequestExecutor(Long sessionId) {
        return ServerSessionRequestExecutor.newInstance(publisherFactory.get(), this, sessionId);
    }
}
