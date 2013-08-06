package edu.uw.zookeeper.protocol.server;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ExecutorActor;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Message.ServerResponse;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionOperation.Request;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.Response;

public class SessionRequestExecutor extends ExecutorActor<PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> implements TaskExecutor<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> {

    public static SessionRequestExecutor newInstance(
            Executor executor,
            Map<Long, Publisher> listeners,
            Processor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> processor) {
        return new SessionRequestExecutor(executor, listeners, processor);
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final ConcurrentLinkedQueue<PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>> mailbox;
    protected final Processor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> processor;
    protected final Map<Long, Publisher> listeners;
    
    public SessionRequestExecutor(
            Executor executor,
            Map<Long, Publisher> listeners,
            Processor<? super SessionOperation.Request<Records.Request>, ? extends Message.ServerResponse<Records.Response>> processor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = new ConcurrentLinkedQueue<PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>>();
        this.listeners = listeners;
        this.processor = processor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<Records.Response>> submit(SessionOperation.Request<Records.Request> request) {
        PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> task = PromiseTask.<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>>of(request);
        send(task);
        return task;
    }

    @Override
    protected Queue<PromiseTask<Request<edu.uw.zookeeper.protocol.proto.Records.Request>, ServerResponse<Response>>> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected boolean apply(PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> input) {
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Executing {}", input.task());
                    }
                    Message.ServerResponse<Records.Response> response = processor.apply(input.task());
                    Publisher listener = listeners.get(input.task().getSessionId());
                    if (OpCode.CLOSE_SESSION == response.getRecord().getOpcode()) {
                        listeners.remove(input.task().getSessionId());
                    }
                    if (listener != null) {
                        listener.post(response);
                    }
                    input.set(response);
                } catch (Exception e) {
                    input.setException(e);
                }
            } else {
                input.cancel(true);
            }
        }
        return (state() != State.TERMINATED);
    }

    @Override
    protected void doStop() {
        PromiseTask<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> task;
        while ((task = mailbox.poll()) != null) {
            task.cancel(true);
        }
    }
}
