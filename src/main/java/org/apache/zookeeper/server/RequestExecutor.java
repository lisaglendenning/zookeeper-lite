package org.apache.zookeeper.server;

import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.TaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

public class RequestExecutor extends TaskExecutor<OpResultTask, Operation.Result> implements RequestExecutorService {

    protected final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);
    protected final ExecutorService executor;
    protected final Zxid zxid;
    protected final SessionManager sessions;
    
    @Inject
    public RequestExecutor(ExecutorService executor, Zxid zxid, SessionManager sessions) {
        super();
        this.executor = executor;
        this.zxid = zxid;
        this.sessions = sessions;
    }
    
    public ExecutorService executor() {
        return executor;
    }

    public Zxid zxid() {
        return zxid;
    }

    public SessionManager sessions() {
        return sessions;
    }
    
    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) throws InterruptedException {
        logger.debug("Submitting request {}", request);
        OpResultTask task = createTask(request);
        ListenableFuture<Operation.Result> future = submit(task);
        executor().submit(this);
        return future;
    }
    
    public OpResultTask createTask(Operation.Request request) {
        OpRequestTask task;
        switch (request.operation()) {
        case CREATE_SESSION:
            task = OpCreateSessionTask.create(request, sessions());
            break;
        default:
            task = OpRequestTask.create(request);
            break;
        }
        return createTask(task);
    }

    public OpResultTask createTask(OpRequestTask task) {
        return createTask(OpCallResponseTask.create(zxid(), task));
    }
    
    public OpResultTask createTask(OpCallResponseTask task) {
        return OpResultTask.create(task);
    }
    
}
