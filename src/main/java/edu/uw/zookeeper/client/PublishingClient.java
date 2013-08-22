package edu.uw.zookeeper.client;

import com.google.common.base.Objects;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.Operation;

public class PublishingClient<I extends Operation.Request, O extends Operation.ProtocolResponse<?>> implements ClientExecutor<I,O> {

    public static <I extends Operation.Request, O extends Operation.ProtocolResponse<?>> PublishingClient<I,O> create(
            ClientExecutor<? super I, O> delegate, Publisher publisher) {
        return new PublishingClient<I,O>(delegate, publisher);
    }
    
    private final ClientExecutor<? super I, O> delegate;
    private final Publisher publisher;
    
    public PublishingClient(
            ClientExecutor<? super I, O> delegate, 
            Publisher publisher) {
        this.delegate = delegate;
        this.publisher = publisher;
        
        delegate.register(this);
    }

    @Override
    public ListenableFuture<O> submit(I request) {
        publisher.post(new Event(request));
        return delegate.submit(request);
    }

    @Override
    public ListenableFuture<O> submit(I request, Promise<O> promise) {
        publisher.post(new Event(request));
        return delegate.submit(request, promise);
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
    
    @Subscribe
    public void handleResponse(O response) {
        publisher.post(new Event(response));
    }

    public static class Event {
        
        private final long time;
        private final Operation operation;

        public Event(Operation operation) {
            this(System.nanoTime(), operation);
        }
        
        public Event(long time, Operation operation) {
            this.time = time;
            this.operation = operation;
        }
        
        public long getTime() {
            return time;
        }
        
        public Operation getOperation() {
            return operation;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("time", getTime()).add("operation", getOperation()).toString();
        }
    }
}
