package edu.uw.zookeeper.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.ClientMessageExecutor;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Processors.*;

public class ServerExecutor implements ClientMessageExecutor, Callable<Void>, ParameterizedFactory<Long, SessionRequestExecutor> {

    public static ServerExecutor newInstance(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions) {
        Generator<Long> zxids = ZxidIncrementer.newInstance();
        FilteringProcessor<Message.ClientMessage, Message.ServerMessage> createProcessor =
                OpCreateSessionProcessor.filtered(sessions, zxids);
        FilteringProcessor<Message.ClientMessage, Message.ServerMessage> errorProcessor =
                new FilteredProcessor<Message.ClientMessage, Message.ServerMessage>(
                        Predicates.alwaysTrue(),
                        new ErrorProcessor<Message.ClientMessage, Message.ServerMessage>());
        @SuppressWarnings("unchecked")
        Processor<Message.ClientMessage, Message.ServerMessage> processor = 
                FilteredProcessors.newInstance(createProcessor, errorProcessor);
        return new ServerExecutor(executor, publisherFactory, sessions, zxids, processor);
    }
    
    public static class RequestTask extends Pair<Message.ClientMessage, SettableFuture<Message.ServerMessage>> {

        protected RequestTask(ClientMessage first,
                SettableFuture<ServerMessage> second) {
            super(first, second);
        }
        
    }
    
    protected final ListeningExecutorService executor;
    protected final Factory<Publisher> publisherFactory;
    protected final Generator<Long> zxids;
    protected final ExpiringSessionManager sessions;
    protected final Processor<Message.ClientMessage, Message.ServerMessage> processor;
    protected final BlockingQueue<RequestTask> requests;
    protected final ConcurrentMap<Long, ServerSessionRequestExecutor> executors;
    
    protected ServerExecutor(
            ListeningExecutorService executor,
            Factory<Publisher> publisherFactory,
            ExpiringSessionManager sessions,
            Generator<Long> zxids,
            Processor<Message.ClientMessage, Message.ServerMessage> processor) {
        this.executor = executor;
        this.publisherFactory = publisherFactory;
        this.zxids = zxids;
        this.sessions = sessions;
        this.processor = processor;
        this.requests = new LinkedBlockingQueue<RequestTask>();
        this.executors = Maps.newConcurrentMap();
        
        sessions.register(this);
    }
    
    public ListeningExecutorService executor() {
        return executor;
    }
    
    public ExpiringSessionManager sessions() {
        return sessions;
    }
    
    @Override
    public ListenableFuture<Message.ServerMessage> submit(Message.ClientMessage request) {
        RequestTask task = new RequestTask(request, SettableFuture.<Message.ServerMessage>create());
        try {
            requests.put(task);
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
        call();
        return task.second();
    }

    @Override
    public synchronized Void call() {
        RequestTask next = requests.poll();
        if (next != null) {
            Message.ServerMessage result;
            try { 
                result = processor.apply(next.first());
            } catch (Exception e) {
                throw Throwables.propagate(e);
            }
            next.second().set(result);
        }
        return null;
    }

    @Override
    public ServerSessionRequestExecutor get(Long value) {
        ServerSessionRequestExecutor executor = executors.get(value);
        if (executor == null) {
            executor = ServerSessionRequestExecutor.newInstance(publisherFactory.get(), sessions(), value);
            if (executors.putIfAbsent(value, executor) != null) {
                throw new AssertionError();
            }
        }
        return executor;
    }
    
    @Subscribe
    public void handleSessionStateEvent(SessionStateEvent event) {
        Long sessionId = event.session().id();
        ServerSessionRequestExecutor executor = executors.get(sessionId);
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
}
