package edu.uw.zookeeper.util;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;

public class PublisherActor extends AbstractActor<Object> implements Publisher, Reference<Publisher> {

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
    
    public PublisherActor(
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
        try {
            try {
                send(event);
            } catch (IllegalStateException e) {
                flush(event);
            }
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
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
    public synchronized void doRun() throws Exception {
        super.doRun();
    }

    @Override
    protected boolean apply(Object input) {
        get().post(input);
        return true;
    }

    @Override
    protected synchronized void doStop() {
        Object next = null;
        while ((next = mailbox.poll()) != null) {
            try {
                get().post(next);
            } catch (Exception e) {}
        }
    }
    
    protected synchronized void flush(Object input) throws Exception {
        doRun();
        get().post(input);
    }
}
