package edu.uw.zookeeper.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.IntConfiguration;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.Operation;

public class LimitOutstandingClient<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> implements ClientExecutor<I,O> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> LimitOutstandingClient<I,O> create(
            Configuration configuration,
            ClientExecutor<? super I, O> client) {
        return create(newConfiguration().get(configuration), client);
    }

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> LimitOutstandingClient<I,O> create(
            int limit,
            ClientExecutor<? super I, O> client) {
        return new LimitOutstandingClient<I,O>(limit, client);
    }

    public static IntConfiguration newConfiguration() {
        return newConfiguration(DEFAULT_CONFIG_PATH);
    }
    
    public static IntConfiguration newConfiguration(String configPath) {
        return IntConfiguration.newInstance(
                configPath, DEFAULT_CONFIG_KEY, DEFAULT_CONFIG_ARG, DEFAULT_CONFIG_VALUE);
    }
    
    public static final String DEFAULT_CONFIG_PATH = "";
    public static final String DEFAULT_CONFIG_ARG = "outstanding";
    public static final String DEFAULT_CONFIG_KEY = "Outstanding";
    public static final int DEFAULT_CONFIG_VALUE = 1000;

    private final Monitor monitor = new Monitor();
    private final Monitor.Guard notThrottled = new Monitor.Guard(monitor) {
        public boolean isSatisfied() {
          return outstanding < limit;
        }
      };
    private int outstanding = 0;
    private final int limit;
    private final ClientExecutor<? super I, O> delegate;
    private final Listener listener;
    
    public LimitOutstandingClient(
            int limit,
            ClientExecutor<? super I, O> delegate) {
        this.limit = limit;
        this.delegate = delegate;
        this.listener = new Listener();
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getOutstanding() {
        monitor.enter();
        try {
            return outstanding;
        } finally {
            monitor.leave();
        }
    }
    
    @Override
    public ListenableFuture<O> submit(I request) {
        return submit(request, SettableFuturePromise.<O>create());
    }

    @Override
    public ListenableFuture<O> submit(I request, Promise<O> promise) {
        try {
            monitor.enterWhen(notThrottled);
        } catch (InterruptedException e) {
            promise.setException(e);
            return promise;
        }
        
        try {
            ListenableFuture<O> future = delegate.submit(request, promise);
            outstanding += 1;
            future.addListener(listener, MoreExecutors.sameThreadExecutor());
            return future;
        } finally {
            monitor.leave();
        }
    }


    @Override
    public void register(Object handler) {
        delegate.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
    }
    
    private class Listener implements Runnable {
        @Override
        public void run() {
            monitor.enter();
            try {
                assert (outstanding > 0);
                outstanding -= 1;
            } finally {
                monitor.leave();
            }
        }
    }
}
