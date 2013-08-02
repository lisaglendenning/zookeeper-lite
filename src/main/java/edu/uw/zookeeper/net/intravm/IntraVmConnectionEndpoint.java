package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.PublisherActor;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.net.Logging;

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
        this.logger = LogManager.getLogger(getClass());
        this.address = address;
        this.publisher = PublisherActor.newInstance(
                LoggingPublisher.create(logger, publisher, this), executor);
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
                logger.trace(Logging.NET_MARKER, "DROPPING {}", next);
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