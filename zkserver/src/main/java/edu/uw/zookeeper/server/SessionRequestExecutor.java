package edu.uw.zookeeper.server;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.OpCode;

public class SessionRequestExecutor extends ExecutedActor<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> implements TaskExecutor<SessionOperation.Request<?>, Message.ServerResponse<?>> {

    public static SessionRequestExecutor newInstance(
            Executor executor,
            Map<Long, PubSubSupport<Object>> listeners,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor) {
        return new SessionRequestExecutor(executor, listeners, processor);
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final ConcurrentLinkedQueue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> mailbox;
    protected final Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor;
    protected final Map<Long, PubSubSupport<Object>> listeners;
    
    public SessionRequestExecutor(
            Executor executor,
            Map<Long, PubSubSupport<Object>> listeners,
            Processor<? super SessionOperation.Request<?>, ? extends Message.ServerResponse<?>> processor) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = new ConcurrentLinkedQueue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>>();
        this.listeners = listeners;
        this.processor = processor;
    }

    @Override
    public ListenableFuture<Message.ServerResponse<?>> submit(SessionOperation.Request<?> request) {
        PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> task = PromiseTask.<SessionOperation.Request<?>, Message.ServerResponse<?>>of(request);
        send(task);
        return task;
    }

    @Override
    protected Queue<PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>>> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected boolean apply(PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> input) {
        if (! input.isDone()) {
            if (state() != State.TERMINATED) {
                try {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Executing {}", input.task());
                    }
                    Message.ServerResponse<?> response = processor.apply(input.task());
                    PubSubSupport<Object> listener = listeners.get(input.task().getSessionId());
                    if (OpCode.CLOSE_SESSION == response.record().opcode()) {
                        listeners.remove(input.task().getSessionId());
                    }
                    if (listener != null) {
                        listener.publish(response);
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
        PromiseTask<SessionOperation.Request<?>, Message.ServerResponse<?>> task;
        while ((task = mailbox.poll()) != null) {
            task.cancel(true);
        }
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
