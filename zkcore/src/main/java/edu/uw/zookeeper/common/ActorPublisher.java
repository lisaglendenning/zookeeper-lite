package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.Executor;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;

public class ActorPublisher<T> extends CallbackActor<T> implements PubSubSupport<T> {

    public static <T> ActorPublisher<T> newPublisher(
            PubSubSupport<T> publisher,
            Executor executor) {
        return newPublisher(
                publisher,
                executor,
                LogManager.getLogger(ActorPublisher.class));
    }

    public static <T> ActorPublisher<T> newPublisher(
            PubSubSupport<T> publisher,
            Executor executor,
            Logger logger) {
        return new ActorPublisher<T>(
                publisher,
                executor,
                Queues.<T>newConcurrentLinkedQueue(),
                logger);
    }

    protected final Executor executor;
    protected final Queue<T> mailbox;
    protected final PubSubSupport<T> publisher;
    protected final Logger logger;
    
    protected ActorPublisher(
            PubSubSupport<T> publisher,
            Executor executor, 
            Queue<T> mailbox,
            Logger logger) {
        super();
        this.publisher = publisher;
        this.mailbox = mailbox;
        this.executor = executor;
        this.logger = logger;
    }

    @Override
    public void publish(T event) {
        if (! send(event)) {
            flush(event);
        }
    }
    
    @Override
    public void subscribe(Object listener) {
        publisher.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return publisher.unsubscribe(listener);
    }
    
    @Override
    protected boolean apply(T input) {
        publisher.publish(input);
        return true;
    }

    @Override
    protected Queue<T> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }

    @Override
    protected Logger logger() {
        return logger;
    }
    
    protected PubSubSupport<T> publisher() {
        return publisher;
    }
}
