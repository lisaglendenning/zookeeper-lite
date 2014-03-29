package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.WeakConcurrentSet;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractActor;
import edu.uw.zookeeper.common.LoggingPromise;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.SameThreadExecutor;
import edu.uw.zookeeper.common.SettableFuturePromise;

public class TimeOutActor<T,V> extends AbstractActor<T> implements ListenableFuture<V> {

    public static <T,V> TimeOutActor<T,V> create(
            TimeOutParameters parameters,
            ScheduledExecutorService executor) {
        Logger logger = LogManager.getLogger(TimeOutActor.class);
        return new TimeOutActor<T,V>(
                parameters, 
                executor,
                new WeakConcurrentSet<Pair<Runnable,Executor>>(),
                LoggingPromise.create(logger, SettableFuturePromise.<V>create()),
                logger);
    }
    
    protected static final long NEVER_TIMEOUT = 0L;

    protected final ScheduledExecutorService scheduler;
    protected final TimeOutParameters parameters;
    protected final Promise<V> promise;
    protected final IConcurrentSet<Pair<Runnable,Executor>> listeners;
    protected volatile ScheduledFuture<?> scheduled;
    
    protected TimeOutActor(
            TimeOutParameters parameters,
            ScheduledExecutorService executor,
            IConcurrentSet<Pair<Runnable,Executor>> listeners,
            Promise<V> promise,
            Logger logger) {
        super(logger);
        this.parameters = checkNotNull(parameters);
        this.scheduler = checkNotNull(executor);
        this.promise = checkNotNull(promise);
        this.listeners = checkNotNull(listeners);
        this.scheduled = null;
        
        promise.addListener(this, SameThreadExecutor.getInstance());
    }

    @Override
    public synchronized void addListener(Runnable listener, Executor executor) {
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
    protected boolean doSend(T message) {
        parameters.touch();
        return true;
    }

    @Override
    protected synchronized void doSchedule() {
        if (parameters.getTimeOut() != NEVER_TIMEOUT) {
            if (!isDone() && !scheduler.isShutdown() && (listeners.size() > 0)) {
                scheduled = scheduler.schedule(this, parameters.getTimeOut(), parameters.getUnit());
            } else {
                stop();
            }
        } else {
            state.compareAndSet(State.SCHEDULED, State.WAITING);
        }
    }

    @Override
    protected void doRun() {
        if (parameters.remaining() <= 0) {
            promise.setException(new TimeoutException());
        }
        if (isDone()) {
            stop();
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
        if (scheduled != null) {
            scheduled.cancel(false);
        }
        promise.cancel(true);
        Iterator<Pair<Runnable,Executor>> itr = Iterators.consumingIterator(listeners.iterator());
        while (itr.hasNext()) {
            Pair<Runnable,Executor> listener = itr.next();
            listener.second().execute(listener.first());
        }
    }
}
