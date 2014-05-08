package edu.uw.zookeeper.common;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Throwables;


public abstract class AbstractActor<T> implements Actor<T> {
    
    protected final Logger logger;
    protected final AtomicReference<State> state;
    
    protected AbstractActor(Logger logger) {
        this(logger, State.WAITING);
    }

    protected AbstractActor(Logger logger, State state) {
        this.logger = logger;
        this.state = new AtomicReference<State>(state);
    }

    @Override
    public boolean send(T message) {
        if (state() == State.TERMINATED) {
            return false;
        } else {
            logger.debug("Received {} ({})", message, this);
            return doSend(message);
        }
    }
    
    protected abstract boolean doSend(T message);
    
    @Override
    public State state() {
        return state.get();
    }

    @Override
    public void run() {
        try {
            if (runEnter()) {
                logger.trace("Running ({})", this);
                doRun();
                runExit();
            }
        } catch (Throwable e) {
            logger.warn("Unhandled error ({})", this, e);
            stop();
            throw Throwables.propagate(e);
        }
    }

    protected boolean runEnter() {
        if (schedule()) {
            return false;
        } else {
            return state.compareAndSet(State.SCHEDULED, State.RUNNING);
        }
    }

    protected abstract void doRun() throws Exception;
    
    protected void runExit() {
        state.compareAndSet(State.RUNNING, State.WAITING);
    }

    @Override
    public boolean stop() {
        boolean stop = (state() != State.TERMINATED)
                && (state.getAndSet(State.TERMINATED) != State.TERMINATED);
        if (stop) {
            logger.debug("Stopping ({})", this);
            doStop();
        }
        return stop;
    }
    
    protected abstract void doStop();

    protected boolean schedule() {
        if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
            logger.trace("Scheduling ({})", this);
            doSchedule();
            return true;
        }
        return false;
    }
    
    protected void doSchedule() {
        run();
    }
}
