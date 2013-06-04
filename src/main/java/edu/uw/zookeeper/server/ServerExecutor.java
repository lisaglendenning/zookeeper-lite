package edu.uw.zookeeper.server;

import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Actor;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Reference;
import edu.uw.zookeeper.util.Processors.*;

public class ServerExecutor implements ClientMessageExecutor, Executor, ParameterizedFactory<Long, ServerExecutor.PublishingSessionRequestExecutor> {

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

    public static class ClientMessageTask extends PromiseTask<ProcessorThunk<Message.ClientMessage, Message.ServerMessage>, Message.ServerMessage>
            implements RunnableFuture<Message.ServerMessage> {
        public static ClientMessageTask newInstance(
                Processor<? super Message.ClientMessage, ? extends Message.ServerMessage> first,
                Message.ClientMessage second) {
            Promise<Message.ServerMessage> promise = newPromise();
            return newInstance(first, second, promise);
        }
        
        public static ClientMessageTask newInstance(
                Processor<? super Message.ClientMessage, ? extends Message.ServerMessage> first,
                Message.ClientMessage second,
                Promise<Message.ServerMessage> promise) {
            ProcessorThunk<Message.ClientMessage, Message.ServerMessage> task = ProcessorThunk.newInstance(first, second);
            return new ClientMessageTask(task, promise);
        }
        
        public ClientMessageTask(
                ProcessorThunk<Message.ClientMessage, Message.ServerMessage> task,
                Promise<Message.ServerMessage> promise) {
            super(task, promise);
        }
        
        @Override
        public synchronized void run() {
            if (! isDone()) {
                Message.ServerMessage result;
                try {
                    result = task().call();
                } catch (Exception e) {
                    setException(e);
                    return;
                }
                set(result);
            }
        }
    }
    
    public static class ServerClientMessageExecutor extends AbstractActor<ClientMessageTask, Void> implements ClientMessageExecutor {
        
        public static ServerClientMessageExecutor newInstance(
                Reference<Long> zxids,
                SessionTable sessions,
                Executor executor) {
            return newInstance(processor(zxids, sessions), executor);
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
                Processor<Message.ClientMessage, Message.ServerMessage> processor,
                Executor executor) {
            return new ServerClientMessageExecutor(
                    processor, executor, AbstractActor.<ClientMessageTask>newQueue(), newState());
        }
        
        protected final Processor<Message.ClientMessage, Message.ServerMessage> processor;
        
        protected ServerClientMessageExecutor(
                Processor<Message.ClientMessage, Message.ServerMessage> processor,
                Executor executor, 
                Queue<ClientMessageTask> mailbox,
                AtomicReference<State> state) {
            super(executor, mailbox, state);
            this.processor = processor;
        }
        
        @Override
        public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request) {
            return submit(request, PromiseTask.<Message.ServerMessage>newPromise());
        }

        @Override
        public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request,
                Promise<Message.ServerMessage> promise) {
            ClientMessageTask task = ClientMessageTask.newInstance(processor, request, promise);
            send(task);
            return task;
        }

        @Override
        protected Void apply(ClientMessageTask input) throws Exception {
            input.run();
            return null;
        }   
    }
    
    protected final ListeningExecutorService executor;
    protected final Factory<Publisher> publisherFactory;
    protected final AssignZxidProcessor zxids;
    protected final ExpiringSessionManager sessions;
    protected final ConcurrentMap<Long, PublishingSessionRequestExecutor> executors;
    protected final ServerClientMessageExecutor anonymousExecutor;
    protected final Actor<Runnable> tasks;
    
    protected ServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            AssignZxidProcessor zxids) {
        this.executor = executor;
        this.publisherFactory = publisherFactory;
        this.zxids = zxids;
        this.sessions = sessions;
        this.executors = Maps.newConcurrentMap();
        this.tasks = AbstractActor.newInstance(
                new Processor<Runnable, Void>() {
                    @Override
                    public Void apply(Runnable input) throws Exception {
                        input.run();
                        return null;
                    }
                }, executor);
        this.anonymousExecutor = ServerClientMessageExecutor.newInstance(zxids, sessions, this);

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
        tasks.send(task);
    }
    
    @Override
    public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request) {
        return submit(request, PromiseTask.<Message.ServerMessage>newPromise());
    }
    
    @Override
    public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request,
            Promise<Message.ServerMessage> promise) {
        return anonymousExecutor.submit(request);
    }

    @Override
    public synchronized PublishingSessionRequestExecutor get(Long sessionId) {
        // TODO: check session is valid?
        PublishingSessionRequestExecutor executor = executors.get(sessionId);
        if (executor == null) {
            executor = newSessionRequestExecutor(sessionId);
            if (executors.put(sessionId, executor) != null) {
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
