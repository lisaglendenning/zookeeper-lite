package edu.uw.zookeeper.common;

import java.util.concurrent.Callable;

import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;

public abstract class RunnablePromiseTask<T,V> extends PromiseTask<T,V> implements Runnable, Callable<Optional<V>> {

    public RunnablePromiseTask(
            T task, Promise<V> delegate) {
        super(task, delegate);
    }

    @Override
    public void run() {
        try {
            if (!isDone()) {
                Optional<? extends V> result = call();
                if (result.isPresent()) {
                    set(result.get());
                }
            }
        } catch (Throwable t) {
            setException(t);
        }
    }
}
