package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;

public abstract class AbstractActor<I> implements Actor<I> {
    
    public static <I> Queue<I> newQueue() {
        return new ConcurrentLinkedQueue<I>();
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
        if (state() == State.TERMINATED) {
            throw new IllegalStateException(State.TERMINATED.toString());
        } else {
            mailbox.add(message);
            if (! schedule() && (state() == State.TERMINATED)) {
                mailbox.remove(message);
                throw new IllegalStateException(State.TERMINATED.toString());
            }
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
                doRun();
            } catch (Throwable e) {
                stop();
                throw Throwables.propagate(e);
            }
            runExit();
        }
    }

    protected boolean runEnter() {
        return state.compareAndSet(State.SCHEDULED, State.RUNNING);
    }

    protected void doRun() throws Exception {
        I next;
        while ((next = mailbox.poll()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }

    protected boolean apply(I input) throws Exception {
        if (State.TERMINATED == state()) {
            return false;
        }
        return true;
    }

    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (! mailbox.isEmpty()) {
                schedule();
            }
        }
    }

    @Override
    public boolean stop() {
        boolean stopped = (state.get() != State.TERMINATED)
                && (state.getAndSet(State.TERMINATED) != State.TERMINATED);
        if (stopped) {
            doStop();
        }
        return stopped;
    }
    
    protected void doStop() {
        mailbox.clear();
    }

    @Override
    public boolean schedule() {
        boolean schedule = state.compareAndSet(State.WAITING, State.SCHEDULED);
        if (schedule) {
            doSchedule();
        }
        return schedule;
    }
    
    protected void doSchedule() {
        executor.execute(this);
    }
}
