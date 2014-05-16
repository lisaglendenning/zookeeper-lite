package edu.uw.zookeeper.common;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;

public class CallablePromiseTask<T extends Callable<Optional<V>>,V> extends PromiseTask<T,V> implements Runnable {

    public static <T extends Callable<Optional<V>>,V> CallablePromiseTask<T,V> create(
            T task, 
            Promise<V> promise) {
        return new CallablePromiseTask<T,V>(task, promise);
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
}
