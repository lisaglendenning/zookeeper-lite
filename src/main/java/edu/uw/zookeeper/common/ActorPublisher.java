package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class ActorPublisher extends CallbackActor<Object> implements Publisher {

    public static ActorPublisher newInstance(
            Publisher publisher,
            Executor executor) {
        return new ActorPublisher(
                publisher,
                executor,
                new ConcurrentLinkedQueue<Object>());
    }

    protected final Executor executor;
    protected final Queue<Object> mailbox;
    protected final Publisher publisher;
    
    protected ActorPublisher(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox) {
        super();
        this.publisher = publisher;
        this.mailbox = mailbox;
        this.executor = executor;
    }

    @Override
    public void post(Object event) {
        if (! send(event)) {
            flush(event);
        }
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
    protected boolean apply(Object input) {
        publisher.post(input);
        return true;
    }

    @Override
    protected Queue<Object> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }
    
    protected Publisher publisher() {
        return publisher;
    }
}
