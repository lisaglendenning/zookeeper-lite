package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import edu.uw.zookeeper.common.AbstractActor;

public abstract class TimeOutActor<V> extends AbstractActor<V> {

    protected static long NEVER_TIMEOUT = 0L;
    
    protected final ScheduledExecutorService executor;
    protected final TimeOutParameters parameters;
    protected volatile ScheduledFuture<?> future = null;
    
    protected TimeOutActor(
            TimeOutParameters parameters,
            ScheduledExecutorService executor) {
        super();
        this.parameters = checkNotNull(parameters);
        this.executor = checkNotNull(executor);
        this.future = null;
    }

    @Override
    protected boolean doSend(V message) {
        parameters.touch();
        return true;
    }

    @Override
    protected synchronized void doSchedule() {
        if (parameters.getTimeOut() != NEVER_TIMEOUT) {
            if (!executor.isShutdown()) {
                future = executor.schedule(this, parameters.getTimeOut(), parameters.getUnit());
            } else {
                stop();
            }
        } else {
            state.compareAndSet(State.SCHEDULED, State.WAITING);
        }
    }

    @Override
    protected void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            schedule();
        }
    }

    @Override
    protected synchronized void doStop() {
        if ((future != null) && !future.isDone()) {
            future.cancel(false);
        }
    }
}
