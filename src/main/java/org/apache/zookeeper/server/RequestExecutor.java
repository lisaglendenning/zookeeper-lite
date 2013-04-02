package org.apache.zookeeper.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.Pair;
import org.apache.zookeeper.util.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

public class RequestExecutor implements RequestExecutorService, Callable<ListenableFuture<Operation.Result>> {

    public static RequestExecutorService create(ExecutorService executor, Processor<Operation.Request, Operation.Result> processor) {
        return new RequestExecutor(executor, processor);
    }
    
    protected final Logger logger = LoggerFactory.getLogger(RequestExecutor.class);
    protected final BlockingQueue<Pair<Operation.Request, SettableFuture<Operation.Result>>> requests;
    protected final Processor<Operation.Request, Operation.Result> processor;
    protected final ExecutorService executor;

    @Inject
    public RequestExecutor(ExecutorService executor, Processor<Operation.Request, Operation.Result> processor) {
        super();
        this.requests = new LinkedBlockingQueue<Pair<Operation.Request, SettableFuture<Operation.Result>>>();
        this.executor = executor;
        this.processor = processor;
    }
    
    protected ExecutorService executor() {
        return executor;
    }
    
    protected Processor<Operation.Request, Operation.Result> processor() {
        return processor;
    }
    
    protected BlockingQueue<Pair<Operation.Request, SettableFuture<Operation.Result>>> requests() {
        return requests;
    }

    @Override
    public ListenableFuture<Operation.Result> submit(Operation.Request request) throws InterruptedException {
        logger.debug("Submitting request {}", request);
        SettableFuture<Operation.Result> future = SettableFuture.create();
        Pair<Operation.Request, SettableFuture<Operation.Result>> task = Pair.create(request, future);
        requests().put(task);
        executor().submit(this);
        return future;
    }
    
    public ListenableFuture<Operation.Result> call() throws Exception {
        Pair<Operation.Request, SettableFuture<Operation.Result>> request = requests().poll();
        if (request != null) {
            Operation.Result result = null;
            try {
                result = processor().apply(request.first());
            } catch (Throwable t) {
                request.second().setException(t);
                return request.second();
            }
            request.second().set(result);
            return request.second();
        } else {
            return null;
        }
    }
}
