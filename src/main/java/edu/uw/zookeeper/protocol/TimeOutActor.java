package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import edu.uw.zookeeper.common.Actor;

public abstract class TimeOutActor<V> implements Actor<V> {
    
    protected final ScheduledExecutorService executor;
    protected final AtomicReference<State> state;
    protected final TimeOutParameters parameters;
    protected volatile ScheduledFuture<?> future = null;
    
    protected TimeOutActor(
            TimeOutParameters parameters,
            ScheduledExecutorService executor) {
        this.state = new AtomicReference<State>(State.WAITING);
        this.parameters = checkNotNull(parameters);
        this.executor = checkNotNull(executor);
        this.future = null;
    }

    @Override
    public State state() {
        return state.get();
    }
    
    @Override
    public boolean send(V message) {
        parameters.touch();
        return false;
    }

    @Override
    public void run() {
        if (!state.compareAndSet(State.SCHEDULED, State.RUNNING)) {
            schedule();
            return;
        }
        
        doRun();
        
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            schedule();
        }
    }
    
    protected abstract void doRun();

    @Override
    public boolean stop() {
        if (state.getAndSet(State.TERMINATED) != State.TERMINATED) {
            doStop();
            return true;
        }
        return false;
    }
    
    protected synchronized void doStop() {
        if ((future != null) && !future.isDone()) {
            future.cancel(false);
        }
    }
    
    protected boolean schedule() {
        if (state.compareAndSet(State.WAITING, State.SCHEDULED)) {
            doSchedule();
            return true;
        }
        return false;
    }
    
    protected abstract void doSchedule();
}
