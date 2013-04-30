package edu.uw.zookeeper.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.ServerExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ClientMessage;
import edu.uw.zookeeper.protocol.Message.ServerMessage;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;
import edu.uw.zookeeper.util.Generator;
import edu.uw.zookeeper.util.Pair;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Processors.*;

public class Server implements ServerExecutor, Callable<Void> {

    public static Server newInstance(
            SessionTable sessions) {
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
        return new Server(sessions, zxids, processor);
    }
    
    public static class RequestTask extends Pair<Message.ClientMessage, SettableFuture<Message.ServerMessage>> {

        protected RequestTask(ClientMessage first,
                SettableFuture<ServerMessage> second) {
            super(first, second);
        }
        
    }
    
    protected final Generator<Long> zxids;
    protected final SessionTable sessions;
    protected final Processor<Message.ClientMessage, Message.ServerMessage> processor;
    protected final BlockingQueue<RequestTask> requests;
    
    protected Server(
            SessionTable sessions,
            Generator<Long> zxids,
            Processor<Message.ClientMessage, Message.ServerMessage> processor) {
        this.zxids = zxids;
        this.sessions = sessions;
        this.processor = processor;
        this.requests = new LinkedBlockingQueue<RequestTask>();
    }
    
    public SessionTable sessions() {
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
}
