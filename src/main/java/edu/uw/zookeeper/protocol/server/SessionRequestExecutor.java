package edu.uw.zookeeper.protocol.server;

import java.util.Map;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.PromiseTask;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TaskExecutor;

public class SessionRequestExecutor extends AbstractActor<PromiseTask<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>>> implements TaskExecutor<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>> {

    public static SessionRequestExecutor newInstance(
            Executor executor,
            Map<Long, Publisher> listeners,
            Processor<? super SessionOperation.Request<Records.Request>, ? extends Operation.ProtocolResponse<Records.Response>> processor) {
        return new SessionRequestExecutor(executor, listeners, processor);
    }
    
    protected final Logger logger;
    protected final Processor<? super SessionOperation.Request<Records.Request>, ? extends Operation.ProtocolResponse<Records.Response>> processor;
    protected final Map<Long, Publisher> listeners;
    
    public SessionRequestExecutor(
            Executor executor,
            Map<Long, Publisher> listeners,
            Processor<? super SessionOperation.Request<Records.Request>, ? extends Operation.ProtocolResponse<Records.Response>> processor) {
        super(executor, AbstractActor.<PromiseTask<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>>>newQueue(), AbstractActor.newState());
        this.logger = LoggerFactory.getLogger(getClass());
        this.listeners = listeners;
        this.processor = processor;
    }

    @Override
    public ListenableFuture<Operation.ProtocolResponse<Records.Response>> submit(SessionOperation.Request<Records.Request> request) {
        PromiseTask<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>> task = PromiseTask.<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>>of(request);
        send(task);
        return task;
    }

    @Override
    protected boolean apply(PromiseTask<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>> input) throws Exception {
        boolean running = super.apply(input);
        if (running && !input.isDone()) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Executing {}", input.task());
                }
                Publisher listener = listeners.get(input.task().getSessionId());
                Operation.ProtocolResponse<Records.Response> response = processor.apply(input.task());
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
        }
        return running;
    }

    @Override
    protected void doStop() {
        PromiseTask<SessionOperation.Request<Records.Request>, Operation.ProtocolResponse<Records.Response>> task;
        while ((task = mailbox.poll()) != null) {
            task.cancel(true);
        }
    }
}
