package edu.uw.zookeeper.common;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;

public class CallablePromiseTask<T extends Callable<Optional<V>>,V> extends PromiseTask<T,V> implements Runnable {

    public static <T extends Callable<Optional<V>>,V> CallablePromiseTask<T,V> run(
            T task, 
            Promise<V> promise) {
        CallablePromiseTask<T,V> instance = create(task, promise);
        instance.run();
        return instance;
    }
    
    public static <T extends Callable<Optional<V>>,V> CallablePromiseTask<T,V> create(
            T task, 
            Promise<V> promise) {
        return new CallablePromiseTask<T,V>(task, promise);
    }
    
    public static <T extends Callable<Optional<V>> & ListenableFuture<?>,V> CallablePromiseTask<T,V> listen(
            T task, 
            Promise<V> promise) {
        return listen(create(task, promise));
    }
    
    public static <T extends Callable<Optional<V>> & ListenableFuture<?>,V> CallablePromiseTask<T,V> listenSynchronized(
            T task, 
            Promise<V> promise) {
        return listen(createSynchronized(task, promise));
    }
    
    public static <T extends Callable<Optional<V>> & ListenableFuture<?>,V> CallablePromiseTask<T,V> listen(
            CallablePromiseTask<T,V> task) {
        task.task().addListener(task, MoreExecutors.directExecutor());
        return task;
    }

    public static <T extends Callable<Optional<V>>,V> SynchronizedCallablePromiseTask<T,V> runSynchronized(
            T task, 
            Promise<V> promise) {
        SynchronizedCallablePromiseTask<T,V> instance = createSynchronized(task, promise);
        instance.run();
        return instance;
    }
    
    public static <T extends Callable<Optional<V>>,V> SynchronizedCallablePromiseTask<T,V> createSynchronized(
            T task, 
            Promise<V> promise) {
        return new SynchronizedCallablePromiseTask<T,V>(task, promise);
    }
    
    protected CallablePromiseTask(
            T task, 
            Promise<V> promise) {
        super(task, promise);
    }

    @Override
    public void run() {
        try {
            if (!isDone()) {
                Optional<? extends V> result = task().call();
                if (result.isPresent()) {
                    set(result.get());
                }
            }
        } catch (CancellationException e) { 
            cancel(false);
        } catch (Throwable t) {
            setException(t);
        }
    }
    
    public static class SynchronizedCallablePromiseTask<T extends Callable<Optional<V>>,V> extends CallablePromiseTask<T,V> {

        protected SynchronizedCallablePromiseTask(
                T task, 
                Promise<V> promise) {
            super(task, promise);
        }

        @Override
        public synchronized void run() {
            super.run();
        }
    }
}
