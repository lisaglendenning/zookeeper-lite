package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.LoggingFutureListener;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SettableFuturePromise;

public class TimeOutActor<T,V> extends AbstractActor<T> implements ListenableFuture<V> {

    public static <T,V> TimeOutActor<T,V> create(
            TimeOutParameters parameters,
            ScheduledExecutorService scheduler,
            Logger logger) {
        TimeOutActor<T,V> actor = new TimeOutActor<T,V>(
                checkNotNull(parameters), 
                checkNotNull(scheduler),
                Sets.<Pair<Runnable,Executor>>newHashSet(),
                SettableFuturePromise.<V>create(),
                logger);
        LoggingFutureListener.listen(checkNotNull(logger), actor);
        return actor;
    }
    
    protected static final long NO_TIMEOUT = 0L;

    protected final ScheduledExecutorService scheduler;
    protected final TimeOutParameters parameters;
    protected final Promise<V> promise;
    protected final Set<Pair<Runnable,Executor>> listeners;
    protected Optional<? extends ScheduledFuture<?>> scheduled;
    
    protected TimeOutActor(
            TimeOutParameters parameters,
            ScheduledExecutorService scheduler,
            Set<Pair<Runnable,Executor>> listeners,
            Promise<V> promise,
            Logger logger) {
        super(logger);
        this.parameters = parameters;
        this.scheduler = scheduler;
        this.promise = promise;
        this.listeners = listeners;
        this.scheduled = Optional.absent();
        
        promise.addListener(this, MoreExecutors.directExecutor());
    }

    @Override
    public synchronized void addListener(Runnable listener, Executor executor) {
        // Why manually manage listeners?
        if (isDone()) {
            executor.execute(listener);
        } else {
            listeners.add(Pair.create(listener, executor));
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return promise.cancel(mayInterruptIfRunning);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        return promise.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException {
        return promise.get(timeout, unit);
    }

    @Override
    public boolean isCancelled() {
        return promise.isCancelled();
    }

    @Override
    public boolean isDone() {
        return promise.isDone();
    }
    
    @Override
    public String toString() {
        return toStringHelper().toString();
    }
    
    @Override
    protected synchronized boolean doSend(T message) {
        if (parameters.getTimeOut() != NO_TIMEOUT) {
            parameters.setTouch();
            schedule();
        }
        return true;
    }

    @Override
    protected synchronized void doSchedule() {
        if (isDone() || scheduler.isShutdown()) {
            stop();
        } else if (parameters.getTimeOut() == NO_TIMEOUT) {
            state.compareAndSet(State.SCHEDULED, State.WAITING);
        } else if (!scheduled.isPresent()) {
            long tick = nextTick();
            TimeUnit unit = parameters.getUnit();
            logger.trace("Scheduling for {} {} ({})", tick, unit, this);
            scheduled = Optional.of(scheduler.schedule(this, tick, unit));
        }
    }

    @Override
    protected synchronized void doRun() {
        if (parameters.getRemaining() <= 0L) {
            promise.setException(new TimeoutException(toString()));
        }
    }
    
    @Override
    protected synchronized void runExit() {
        if (state.compareAndSet(State.RUNNING, State.WAITING)) {
            if (scheduled.isPresent()) {
                scheduled = Optional.absent();
            }
            schedule();
        }
    }

    @Override
    protected synchronized void doStop() {
        cancel(true);
        if (scheduled.isPresent()) {
            scheduled.get().cancel(false);
            scheduled = Optional.absent();
        }
        Iterator<Pair<Runnable,Executor>> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            Pair<Runnable,Executor> listener = itr.next();
            listener.second().execute(listener.first());
        }
    }
    
    protected synchronized long nextTick() {
        return Math.max(parameters.getRemaining(), 0L);
    }
    
    protected synchronized MoreObjects.ToStringHelper toStringHelper() {
        return MoreObjects.toStringHelper(this).add("parameters", parameters);
    }
}
