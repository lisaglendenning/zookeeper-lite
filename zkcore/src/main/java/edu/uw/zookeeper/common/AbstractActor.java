package edu.uw.zookeeper.common;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Throwables;


public abstract class AbstractActor<T> implements Actor<T> {
    
    protected final AtomicReference<State> state;
    
    protected AbstractActor() {
        this.state = new AtomicReference<State>(State.WAITING);
    }
    
    protected abstract Logger logger();
    
    protected abstract Queue<T> mailbox();
    
    @Override
    public boolean send(T message) {
        if (state() == State.TERMINATED) {
            return false;
        } else {
            mailbox().add(message);
            if (! schedule() && (state() == State.TERMINATED)) {
                mailbox().remove(message);
                return false;
            } else {
                return true;
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
                logger().warn("Unhandled error", e);
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
        T next;
        while ((next = mailbox().poll()) != null) {
            if (! apply(next)) {
                break;
            }
        }
    }

    protected abstract boolean apply(T input) throws Exception;

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
