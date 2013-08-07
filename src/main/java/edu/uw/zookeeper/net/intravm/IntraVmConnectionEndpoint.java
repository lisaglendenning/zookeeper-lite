package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ExecutorActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.PublisherActor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Logging;

public class IntraVmConnectionEndpoint<T extends SocketAddress> extends ExecutorActor<Optional<Object>> implements Publisher, Executor {

    public static <T extends SocketAddress> IntraVmConnectionEndpoint<T> create(
            T address,
            Publisher publisher,
            Executor executor) {
        return new IntraVmConnectionEndpoint<T>(address, publisher, executor, new ConcurrentLinkedQueue<Optional<Object>>());
    }
    
    protected final Logger logger;
    protected final Executor executor;
    protected final Queue<Optional<Object>> mailbox;
    protected final PublisherActor publisher;
    protected final T address;
    protected final Promise<IntraVmConnectionEndpoint<T>> stopped;
    
    public IntraVmConnectionEndpoint(
            T address,
            Publisher publisher,
            Executor executor,
            Queue<Optional<Object>> mailbox) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.executor = executor;
        this.mailbox = mailbox;
        this.address = address;
        this.publisher = PublisherActor.newInstance(
                LoggingPublisher.create(logger, publisher, this), executor);
        this.stopped = LoggingPromise.create(
                logger, SettableFuturePromise.<IntraVmConnectionEndpoint<T>>create());
    }
    
    public ListenableFuture<IntraVmConnectionEndpoint<T>> stopped() {
        return stopped;
    }
    
    public T address() {
        return address;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public void post(Object object) {
        publisher.post(object);
    }

    @Override
    public void register(Object object) {
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        publisher.unregister(object);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(address()).toString();
    }

    @Override
    protected boolean apply(Optional<Object> input) {
        if (state() != State.TERMINATED) {
            if (input.isPresent()) {
                Object message = input.get();
                post(message);
                return true;
            } else {
                stop();
                return false;
            }
        } else {
            if (logger.isTraceEnabled()) {
                if (input.isPresent()) {
                    logger.trace(Logging.NET_MARKER, "DROPPING {}", input.get());
                }
            }
            return false;
        }
    }

    @Override
    protected void doStop() {
        if (logger.isTraceEnabled()) {
            Optional<Object> next;
            while ((next = mailbox.poll()) != null) {
                if (next.isPresent()) {
                    logger.trace(Logging.NET_MARKER, "DROPPING {}", next.get());
                }
            }
        }
        mailbox.clear();

        publisher.stop();
        
        stopped.set(this);
    }
    
    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Queue<Optional<Object>> mailbox() {
        return mailbox;
    }
    
    protected Publisher publisher() {
        return publisher;
    }
}