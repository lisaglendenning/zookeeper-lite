package edu.uw.zookeeper.server;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.TaskExecutor;
import edu.uw.zookeeper.protocol.ConnectMessage;
import edu.uw.zookeeper.protocol.ZxidReference;

public class SimpleConnectExecutor<T extends AbstractSessionExecutor> implements TaskExecutor<ConnectMessage.Request, ConnectMessage.Response> {
    
    public static <T extends AbstractSessionExecutor> SimpleConnectExecutor<T> defaults(
            Map<Long, T> executors,
            SessionManager sessions,
            ZxidReference lastZxid) {
        ConnectMessageProcessor processor = ConnectMessageProcessor.defaults(sessions, lastZxid);
        return create(executors, processor);
    }
    
    public static <T extends AbstractSessionExecutor> SimpleConnectExecutor<T> create(
            Map<Long, T> executors,
            Function<ConnectMessage.Request, ConnectMessage.Response> processor) {
        return new SimpleConnectExecutor<T>(executors, processor);
    }
    
    protected final Map<Long, T> executors;
    protected final Function<ConnectMessage.Request, ConnectMessage.Response> processor;
    
    protected SimpleConnectExecutor(
            Map<Long, T> executors,
            Function<ConnectMessage.Request, ConnectMessage.Response> processor) {
        this.executors = executors;
        this.processor = processor;
    }

    @Override
    public ListenableFuture<ConnectMessage.Response> submit(ConnectMessage.Request request) {
        if (request instanceof ConnectMessage.Request.RenewRequest) {
            T executor = executors.get(Long.valueOf(request.getSessionId()));
            if (executor != null) {
                executor.timer().send(request);
            }
        }
        ConnectMessage.Response response;
        try { 
            response = processor.apply(request);
        } catch (IllegalArgumentException e) {
            return Futures.immediateFailedFuture(e.getCause());
        }
        return Futures.immediateFuture(response);
    }
}