package org.apache.zookeeper.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

public class TaskExecutor<T extends Callable<V>, V> implements Callable<V> {
    
    protected final BlockingQueue<ListenableFutureTask<V>> tasks;
    
    public TaskExecutor() {
        this(new LinkedBlockingQueue<ListenableFutureTask<V>>());
    }
    
    public TaskExecutor(BlockingQueue<ListenableFutureTask<V>> tasks) {
        this.tasks = tasks;
    }

    public ListenableFuture<V> submit(T task) throws InterruptedException {
        ListenableFutureTask<V> future = ListenableFutureTask.create(task);
        return submit(future);
    }
    
    protected ListenableFuture<V> submit(ListenableFutureTask<V> task) throws InterruptedException {
        tasks.put(task);
        return task;
    }
    
    public V call() throws Exception {
        ListenableFutureTask<V> task = tasks.poll();
        if (task != null) {
            task.run();
            return task.get();
        } else {
            return null;
        }
    }

}
