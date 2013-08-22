package edu.uw.zookeeper.client;

import java.util.concurrent.Executor;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.protocol.Operation;

public class LatencyMeasuringClient<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> implements ClientExecutor<I,O> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> LatencyMeasuringClient<I,O> create(
            ClientExecutor<? super I, O> delegate, Publisher publisher) {
        return new LatencyMeasuringClient<I,O>(delegate, publisher);
    }
    
    private final ClientExecutor<? super I, O> delegate;
    private final Executor executor;
    private final Publisher publisher;
    
    public LatencyMeasuringClient(
            ClientExecutor<? super I, O> delegate, 
            Publisher publisher) {
        this.delegate = delegate;
        this.publisher = publisher;
        this.executor = MoreExecutors.sameThreadExecutor();
    }

    @Override
    public ListenableFuture<O> submit(I request) {
        return submit(request, SettableFuturePromise.<O>create());
    }

    @Override
    public ListenableFuture<O> submit(I request, Promise<O> promise) {
        long start = System.nanoTime();
        ListenableFuture<O> future = delegate.submit(request, promise);
        Listener listener = new Listener(start, request, future);
        future.addListener(listener, executor);
        return future;
    }

    @Override
    public void register(Object handler) {
        delegate.register(handler);
        publisher.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
        try {
            publisher.unregister(handler);
        } catch (IllegalArgumentException e) {}
    }

    public static class Measurement<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> {
        
        public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> Measurement<I,O> create(
                long nanos, I request, O response) {
            return new Measurement<I,O>(nanos, request, response);
        }
            
        private final long nanos;
        private final I request;
        private final O response;
        
        public Measurement(long nanos, I request, O response) {
            super();
            this.nanos = nanos;
            this.request = request;
            this.response = response;
        }

        public long getNanos() {
            return nanos;
        }

        public I getRequest() {
            return request;
        }

        public O getResponse() {
            return response;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("nanos", getNanos()).add("request", getRequest()).add("response", getResponse()).toString();
        }
    }

    public class Listener implements Runnable {

        private final long start;
        private final I request;
        private final ListenableFuture<O> future;
        
        public Listener(
                long start, 
                I request, 
                ListenableFuture<O> future) {
            this.start = start;
            this.request = request;
            this.future = future;
        }

        @Override
        public void run() {
            long nanos = System.nanoTime() - start;
            if (future.isDone() && ! future.isCancelled()) {
                try {
                    O response = future.get();
                    publisher.post(Measurement.create(nanos, request, response));
                } catch (Throwable t) {}
            }
        }
    }
}
