package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.AbstractActor;
import edu.uw.zookeeper.util.LoggingPublisher;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.PublisherActor;
import edu.uw.zookeeper.util.SettableFuturePromise;

public class IntraVmConnectionEndpoint<T extends SocketAddress> extends AbstractActor<Optional<Object>> implements Publisher, Executor {

    public static <T extends SocketAddress> IntraVmConnectionEndpoint<T> create(
            T address,
            Publisher publisher,
            Executor executor) {
        return new IntraVmConnectionEndpoint<T>(address, publisher, executor);
    }
    
    protected final Logger logger;
    protected final PublisherActor publisher;
    protected final T address;
    protected final Promise<Void> stopped;
    
    public IntraVmConnectionEndpoint(
            T address,
            Publisher publisher,
            Executor executor) {
        super(executor, AbstractActor.<Optional<Object>>newQueue(), newState());
        this.logger = LoggerFactory.getLogger(getClass());
        this.address = address;
        this.publisher = PublisherActor.newInstance(
                new LoggingPublisher(logger, publisher, new Function<Object, String>() {
                    @Override
                    public String apply(@Nullable Object input) {
                        return String.format("Posting: %s (%s)", input, IntraVmConnectionEndpoint.this);
                    }
                }), executor);
        this.stopped = SettableFuturePromise.create();
    }
    
    public ListenableFuture<Void> stopped() {
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
    protected boolean apply(Optional<Object> input) throws Exception {
        boolean running = super.apply(input);
        if (running) {
            if (input.isPresent()) {
                Object message = input.get();
                post(message);
            } else {
                stop();
                running = false;
            }
        }
        return running;
    }

    @Override
    protected void doStop() {
        if (logger.isTraceEnabled()) {
            for (Object next: mailbox.toArray()) {
                logger.trace("Dropping {}", next);
            }
        }
        mailbox.clear();

        publisher.stop();
        
        stopped.set(null);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(address()).toString();
    }
}