package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ActorPublisher extends CallbackActor<Object> implements Publisher {

    public static ActorPublisher newInstance(
            Publisher publisher,
            Executor executor) {
        return new ActorPublisher(
                publisher,
                executor,
                new ConcurrentLinkedQueue<Object>(),
                LogManager.getLogger(ActorPublisher.class));
    }

    protected final Executor executor;
    protected final Queue<Object> mailbox;
    protected final Publisher publisher;
    protected final Logger logger;
    
    protected ActorPublisher(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox,
            Logger logger) {
        super();
        this.publisher = publisher;
        this.mailbox = mailbox;
        this.executor = executor;
        this.logger = logger;
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

    @Override
    protected Logger logger() {
        return logger;
    }
    
    protected Publisher publisher() {
        return publisher;
    }
}
