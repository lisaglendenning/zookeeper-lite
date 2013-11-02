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
        logger.debug("Received {} ({})", message, this);
        if (state() == State.TERMINATED) {
            return false;
        } else {
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
        if (runEnter()) {
            try {
                doRun();
            } catch (Throwable e) {
                logger.warn("Unhandled error ({})", this, e);
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
            doSchedule();
            return true;
        }
        return false;
    }
    
    protected void doSchedule() {
        run();
    }
}
