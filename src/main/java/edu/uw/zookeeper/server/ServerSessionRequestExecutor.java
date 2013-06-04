package edu.uw.zookeeper.server;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.OpRecord;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionRequestWrapper;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Processors.*;
import edu.uw.zookeeper.util.Publisher;

public class ServerSessionRequestExecutor extends ForwardingEventful implements ServerExecutor.PublishingSessionRequestExecutor, Executor {

    public static ServerSessionRequestExecutor newInstance(
            Publisher publisher,
            ServerExecutor executor,
            long sessionId) {
        return newInstance(publisher, executor, processor(executor, sessionId), sessionId);
    }
    
    public static ServerSessionRequestExecutor newInstance(
            Publisher publisher,
            ServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            long sessionId) {
        return new ServerSessionRequestExecutor(publisher, executor, processor, sessionId);
    }
    
    public static Processor<Operation.SessionRequest, Operation.SessionReply> processor(
            ServerExecutor executor,
            long sessionId) {
        @SuppressWarnings("unchecked")
        Processor<Operation.Request, Operation.Response> responseProcessor = FilteredProcessors
                .newInstance(
                        OpCloseSessionProcessor.filtered(sessionId, executor.sessions()),
                        FilteredProcessor.newInstance(
                                OpRequestProcessor.NotEqualsFilter
                                        .newInstance(OpCode.CLOSE_SESSION),
                                OpRequestProcessor.newInstance()));
        Processor<Operation.Request, Operation.Reply> replyProcessor = OpErrorProcessor.newInstance(responseProcessor);
        Processor<Operation.SessionRequest, Operation.SessionReply> processor = SessionRequestProcessor.newInstance(replyProcessor, executor.zxids());
        return processor;
    }

    public static class SessionRequestTask extends ProcessorThunk<Operation.SessionRequest, Operation.SessionReply> {
        public static SessionRequestTask newInstance(
                Processor<? super Operation.SessionRequest, ? extends Operation.SessionReply> first,
                Operation.SessionRequest second) {
            return new SessionRequestTask(first, second);
        }
        
        public SessionRequestTask(
                Processor<? super Operation.SessionRequest, ? extends Operation.SessionReply> first,
                Operation.SessionRequest second) {
            super(first, second);
        }}
    
    protected final Logger logger = LoggerFactory
            .getLogger(ServerSessionRequestExecutor.class);
    protected final long sessionId;
    protected final ServerExecutor executor;
    protected final Processor<Operation.SessionRequest, Operation.SessionReply> processor;
    
    protected ServerSessionRequestExecutor(
            Publisher publisher,
            ServerExecutor executor,
            Processor<Operation.SessionRequest, Operation.SessionReply> processor,
            long sessionId) {
        super(publisher);
        this.executor = executor;
        this.processor = processor;
        this.sessionId = sessionId;
        
        register(this);
    }
    
    public ServerExecutor executor() {
        return executor;
    }

    @Override
    public void execute(Runnable runnable) {
        executor().execute(runnable);
    }

    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        return submit(request, PromiseTask.<Operation.SessionReply>newPromise());
    }
    
    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request,
            Promise<Operation.SessionReply> promise) {
        touch();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("0x%s: Submitting %s", Long.toHexString(sessionId), request));
        }
        ListenableFutureTask<Operation.SessionReply> task = ListenableFutureTask.create(SessionRequestTask.newInstance(processor, request));
        execute(task);
        return task;
    }
    
    @Override
    public void post(Object event) {
        super.post(event);
    }

    @Subscribe
    public void handleSessionStateEvent(Session.State event) {
        if (Session.State.SESSION_EXPIRED == event) {
            try {
                submit(SessionRequestWrapper.newInstance(0, OpRecord.OpRequest.newInstance(OpCode.CLOSE_SESSION)));
            } catch (Exception e) {
                // TODO
                throw Throwables.propagate(e);
            }
        }
    }

    protected void touch() {
        executor().sessions().touch(sessionId);
    }
}
