package edu.uw.zookeeper.util;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public class PublisherActor extends AbstractActor<Object, Void> implements Publisher, Reference<Publisher> {

    public static PublisherActor newInstance(
            Publisher publisher,
            Executor executor) {
        return new PublisherActor(
                publisher,
                executor,
                newQueue(), 
                newState());
    }

    protected final Publisher publisher;
    
    protected PublisherActor(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox,
            AtomicReference<State> state) {
        super(executor, mailbox, state);
        this.publisher = publisher;
    }

    @Override
    public Publisher get() {
        return publisher;
    }

    @Override
    public void post(Object event) {
        send(event);
    }
    
    @Override
    public void register(Object object) {
        get().register(object);
    }

    @Override
    public void unregister(Object object) {
        get().unregister(object);
    }

    @Override
    protected Void apply(Object input) {
        get().post(input);
        return null;
    }
}
