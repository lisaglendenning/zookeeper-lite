package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;

public abstract class AbstractActor<I,O> implements Actor<I> {
    
    public static <I,O> SimpleActor<I,O> newInstance(
            Processor<? super I, ? extends O> processor, 
            Executor executor) {
        Queue<I> queue = newQueue();
        return newInstance(
                processor,
                executor,
                queue, 
                newState());
    }

    public static <I,O> SimpleActor<I,O> newInstance(
            Processor<? super I, ? extends O> processor, 
            Executor executor, 
            Queue<I> mailbox,
            AtomicReference<State> state) {
        return SimpleActor.newInstance(
                processor,
                executor,
                mailbox,
                state);
    }
    
    public static <I> Queue<I> newQueue() {
        return new LinkedBlockingQueue<I>();
    }
    
    public static AtomicReference<State> newState() {
        return new AtomicReference<State>(State.WAITING);
    }
    
    protected final AtomicReference<State> state;
    protected final Queue<I> mailbox;
    protected final Executor executor;
    
    protected AbstractActor(
            Executor executor, 
            Queue<I> mailbox,
            AtomicReference<State> state) {
        this.executor = checkNotNull(executor);
        this.mailbox = checkNotNull(mailbox);
        this.state = checkNotNull(state);
    }
    
    @Override
    public void send(I message) {        
        if (mailbox.offer(checkNotNull(message))) {
            if (! schedule() && state() == State.TERMINATED) {
                mailbox.remove(message);
                throw new RejectedExecutionException();
            }
        } else {
            throw new RejectedExecutionException();
        }
    }
    
    @Override
    public State state() {
        return state.get();
    }

    @Override
    public void run() {
        if (runEnter()) {
            try {
                runAll();
            } catch (Throwable e) {
                stop();
                throw Throwables.propagate(e);
            }
            runExit();
        }
    }

    @Override
    public boolean stop() {
        if ((state.get() == State.TERMINATED)
                || (state.getAndSet(State.TERMINATED) == State.TERMINATED)) {
            return false;
        }
        mailbox.clear();
        return true;
    }

    @Override
    public boolean schedule() {
        boolean schedule = state.compareAndSet(State.WAITING, State.SCHEDULED);
        if (schedule) {
            executor.execute(this);
        }
        return schedule;
    }
    
    protected boolean runEnter() {
        return state.compareAndSet(State.SCHEDULED, State.RUNNING);
    }
    
    protected void runAll() throws Exception {
        I next;
        while ((next = mailbox.poll()) != null) {
            apply(next);
        }
    }
    
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (! mailbox.isEmpty()) {
                schedule();
            }
        }
    }
    
    protected abstract O apply(I input) throws Exception;
    
    public static class SimpleActor<I,O> extends AbstractActor<I,O> {
        
        public static <I,O> SimpleActor<I,O> newInstance(
                Processor<? super I, ? extends O> processor, 
                Executor executor, 
                Queue<I> mailbox,
                AtomicReference<State> state) {
            return new SimpleActor<I,O>(
                    processor,
                    executor,
                    mailbox,
                    state);
        }
        
        protected final Processor<? super I, ? extends O> processor;
        
        protected SimpleActor(
                Processor<? super I, ? extends O> processor, 
                Executor executor,
                Queue<I> mailbox,
                AtomicReference<Actor.State> state) {
            super(executor, mailbox, state);
            this.processor = checkNotNull(processor);
        }

        @Override
        protected O apply(I input) throws Exception {
            return processor.apply(input);
        }
    }
}
