package edu.uw.zookeeper.client;

import java.util.concurrent.Executor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.ConfigValueType;

import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.Operation;

public class LimitOutstandingClient<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> implements ClientExecutor<I,O> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> ClientExecutor<? super I,O> create(
            Configuration configuration,
            ClientExecutor<? super I, O> client) {
        return create(ConfigurableLimit.get(configuration), client);
    }

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> ClientExecutor<? super I,O> create(
            int limit,
            ClientExecutor<? super I, O> client) {
        if (limit == NO_LIMIT) {
            return client;
        } else if (limit < 0) {
            throw new IllegalStateException(String.valueOf(limit));
        } else {
            return new LimitOutstandingClient<I,O>(limit, client);
        }
    }

    @Configurable(arg="outstanding", key="Outstanding", value="1000", type=ConfigValueType.NUMBER)
    public static class ConfigurableLimit implements Function<Configuration, Integer> {

        public static Integer get(Configuration configuration) {
            return new ConfigurableLimit().apply(configuration);
        }

        @Override
        public Integer apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getInt(configurable.key());
        }
    }
    
    public static int NO_LIMIT = 0;
    
    private final Monitor monitor = new Monitor();
    private final Monitor.Guard notThrottled = new Monitor.Guard(monitor) {
        public boolean isSatisfied() {
          return outstanding < limit;
        }
      };
    private volatile int outstanding = 0;
    private final int limit;
    private final ClientExecutor<? super I, O> delegate;
    private final Listener listener;
    private final Executor executor;
    
    protected LimitOutstandingClient(
            int limit,
            ClientExecutor<? super I, O> delegate) {
        this.limit = limit;
        this.delegate = delegate;
        this.executor = MoreExecutors.sameThreadExecutor();
        this.listener = new Listener();
    }
    
    public int getLimit() {
        return limit;
    }
    
    public int getOutstanding() {
        return outstanding;
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
            future.addListener(listener, executor);
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
    
    @Override
    public void post(Object event) {
        delegate.post(event);
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
