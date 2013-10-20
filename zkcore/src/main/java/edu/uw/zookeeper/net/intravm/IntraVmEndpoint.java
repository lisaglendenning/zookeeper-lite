package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ExecutedActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.net.LoggingMarker;

public class IntraVmEndpoint<V> extends ExecutedActor<Optional<? extends V>> implements PubSubSupport<Object>, Executor {

    public static <V> IntraVmEndpoint<V> create(
            SocketAddress address,
            PubSubSupport<Object> publisher,
            Executor executor) {
        return IntraVmEndpoint.<V>builder(
                address, 
                publisher,
                executor).build();
    }

    public static <V> Builder<V> builder(
            SocketAddress address,
            PubSubSupport<Object> publisher,
            Executor executor) {
        return new Builder<V>(address, publisher, executor);
    }
    
    public static class Builder<V> implements edu.uw.zookeeper.common.Builder<IntraVmEndpoint<V>> {
        
        protected final SocketAddress address;
        protected final Logger logger;
        protected final IntraVmPublisher publisher;
        protected final Executor executor;
        protected final Promise<IntraVmEndpoint<V>> stopped;
        protected final Queue<Optional<? extends V>> mailbox;
        
        public Builder(
                SocketAddress address,
                PubSubSupport<Object> publisher,
                Executor executor) {
            this(address, publisher, executor, LogManager.getLogger(IntraVmEndpoint.class));
        }
        
        public Builder(
                SocketAddress address,
                PubSubSupport<Object> publisher,
                Executor executor,
                Logger logger) {
            this.logger = logger;
            this.address = address;
            this.executor = executor;
            this.publisher = IntraVmPublisher.newInstance(
                    LoggingPublisher.create(
                            logger, publisher, address), 
                    executor,
                    logger);
            this.stopped = LoggingPromise.create(
                    logger, SettableFuturePromise.<IntraVmEndpoint<V>>create());
            this.mailbox = Queues.<Optional<? extends V>>newConcurrentLinkedQueue();
        }
        
        public IntraVmPublisher getPublisher() {
            return publisher;
        }
        
        @Override
        public IntraVmEndpoint<V> build() {
            return new IntraVmEndpoint<V>(address, logger, executor, publisher, mailbox, stopped);
        }
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final Queue<Optional<? extends V>> mailbox;
    protected final IntraVmPublisher publisher;
    protected final SocketAddress address;
    protected final Promise<IntraVmEndpoint<V>> stopped;

    /**
     * @param executor execute order preserves submit order
     */
    protected IntraVmEndpoint(
            SocketAddress address,
            Logger logger,
            Executor executor,
            IntraVmPublisher publisher,
            Queue<Optional<? extends V>> mailbox,
            Promise<IntraVmEndpoint<V>> stopped) {
        super();
        this.logger = logger;
        this.executor = executor;
        this.address = address;
        this.mailbox = mailbox;
        this.publisher = publisher;
        this.stopped = stopped;
    }
    
    public ListenableFuture<? extends IntraVmEndpoint<V>> stopped() {
        return stopped;
    }
    
    public SocketAddress address() {
        return address;
    }
    
    public <U> ListenableFuture<U> write(Optional<U> message, IntraVmEndpoint<?> remote) {
        @SuppressWarnings("unchecked")
        SendTask<U> task = new SendTask<U>((IntraVmEndpoint<? super U>) remote, message, LoggingPromise.create(logger, SettableFuturePromise.<U>create()));
        execute(task);
        return task;
    }

    @Override
    public void execute(Runnable command) {
        executor().execute(command);
    }

    @Override
    public void publish(Object object) {
        publisher.publish(object);
    }

    @Override
    public void subscribe(Object object) {
        publisher.subscribe(object);
    }

    @Override
    public boolean unsubscribe(Object object) {
        return publisher.unsubscribe(object);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("address", address)
                .add("state", state()).toString();
    }

    @Override
    protected synchronized boolean apply(Optional<? extends V> input) {
        if (state() != State.TERMINATED) {
            if (input.isPresent()) {
                doPublish(input.get());
            } else {
                stop();
            }
        } else {
            if (logger.isTraceEnabled()) {
                if (input.isPresent()) {
                    logger.trace(LoggingMarker.NET_MARKER.get(), "DROPPING {}", input.get());
                }
            }
        }
        return (state() != State.TERMINATED);
    }

    protected void doPublish(V event) {
        publisher.publish(event);
    }
    
    @Override
    protected synchronized void doStop() {
        if (logger.isTraceEnabled()) {
            Optional<? extends V> next;
            while ((next = mailbox.poll()) != null) {
                if (next.isPresent()) {
                    logger.trace(LoggingMarker.NET_MARKER.get(), "DROPPING {}", next.get());
                }
            }
        }
        mailbox.clear();
        // synchronized because we want to make sure that we don't post any messages
        // after declaring that we are closed
        stopped.set(this);
    }
    
    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Queue<Optional<? extends V>> mailbox() {
        return mailbox;
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    protected static class SendTask<U> extends PromiseTask<Optional<U>, U> implements Runnable {

        protected final IntraVmEndpoint<? super U> remote;
        
        public SendTask(
                IntraVmEndpoint<? super U> remote,
                Optional<U> task,
                Promise<U> promise) {
            super(task, promise);
            this.remote = remote;
        }
        
        @Override
        public synchronized void run() {
            if (!isDone()) {
                if (remote.send(task)) {
                    set(task.orNull());
                } else {
                    setException(new IllegalStateException(Connection.State.CONNECTION_CLOSING.toString()));
                }
            }
        }
    }
}