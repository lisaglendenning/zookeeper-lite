package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.base.Throwables;

public class PublisherActor extends ExecutorActor<Object> implements Publisher, Reference<Publisher> {

    public static PublisherActor newInstance(
            Publisher publisher,
            Executor executor) {
        return new PublisherActor(
                publisher,
                executor,
                new ConcurrentLinkedQueue<Object>());
    }

    protected final Executor executor;
    protected final Queue<Object> mailbox;
    protected final Publisher publisher;
    
    public PublisherActor(
            Publisher publisher,
            Executor executor, 
            Queue<Object> mailbox) {
        super();
        this.publisher = publisher;
        this.mailbox = mailbox;
        this.executor = executor;
    }

    @Override
    public Publisher get() {
        return publisher;
    }

    @Override
    public void post(Object event) {
        try {
            send(event);
        } catch (RejectedExecutionException e) {
            flush(event);
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
    public synchronized void doRun() {
        try {
            super.doRun();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected synchronized boolean apply(Object input) {
        get().post(input);
        return true;
    }

    protected synchronized void flush(Object input) {
        doRun();
        apply(input);
    }

    @Override
    protected synchronized void doStop() {
        Object next = null;
        while ((next = mailbox.poll()) != null) {
            try {
                apply(next);
            } catch (Exception e) {}
        }
    }
    
    @Override
    protected Queue<Object> mailbox() {
        return mailbox;
    }

    @Override
    protected Executor executor() {
        return executor;
    }
}
