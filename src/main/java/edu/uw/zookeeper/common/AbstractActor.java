package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;


public abstract class AbstractActor<I> implements Actor<I> {
    
    protected final AtomicReference<State> state;
    
    protected AbstractActor() {
        this.state = new AtomicReference<State>(State.WAITING);
    }
    
    protected abstract Queue<I> mailbox();
    
    @Override
    public void send(I message) {
        if (state() == State.TERMINATED) {
            throw new RejectedExecutionException(State.TERMINATED.toString());
        } else {
            mailbox().add(message);
            if (! schedule() && (state() == State.TERMINATED)) {
                mailbox().remove(message);
                throw new RejectedExecutionException(State.TERMINATED.toString());
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
        if (schedule()) {
            return false;
        } else {
            return state.compareAndSet(State.SCHEDULED, State.RUNNING);
        }
    }

    protected void doRun() throws Exception {
        I next;
        while ((next = mailbox().poll()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }

    protected boolean apply(I input) throws Exception {
        return (state() != State.TERMINATED);
    }

    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (! mailbox().isEmpty()) {
                schedule();
            }
        }
    }

    @Override
    public boolean stop() {
        boolean stop = (state() != State.TERMINATED)
                && (state.getAndSet(State.TERMINATED) != State.TERMINATED);
        if (stop) {
            doStop();
        }
        return stop;
    }
    
    protected void doStop() {
        mailbox().clear();
    }

    protected boolean schedule() {
        boolean schedule = state.compareAndSet(State.WAITING, State.SCHEDULED);
        if (schedule) {
            doSchedule();
        }
        return schedule;
    }
    
    protected void doSchedule() {
        run();
    }
}
