package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class CallableMonitor<T> extends TaskMonitor {

    protected class CallableMonitorListener implements FutureCallback<T> {

        protected final ListenableFuture<T> future;

        public CallableMonitorListener(ListenableFuture<T> future) {
            this.future = future;
        }

        @Override
        public void onFailure(Throwable arg0) {
            logger.warn("FAILED: {}", future, arg0);
            wake();
        }

        @Override
        public void onSuccess(Object arg0) {
            logger.debug("TERMINATED ({}): {}", arg0, future);
            wake();
        }
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ServiceMonitor.class);
    protected final Executor listenerExecute = MoreExecutors
            .sameThreadExecutor();
    protected final ListeningExecutorService execute;
    protected final List<Callable<T>> tasks;
    protected final Map<Callable<T>, ListenableFuture<T>> futures;

    public CallableMonitor(ListeningExecutorService executor) {
        synchronized (this) {
            this.execute = executor;
            this.tasks = Collections.synchronizedList(Lists
                    .<Callable<T>> newArrayList());
            this.futures = Collections.synchronizedMap(Maps
                    .<Callable<T>, ListenableFuture<T>> newHashMap());
        }
    }

    public synchronized void add(Callable<T> task) {
        checkState(state() == State.NEW);
        checkArgument(!(tasks.contains(task)));
        tasks.add(task);
    }

    public synchronized ListenableFuture<T> get(Callable<T> task) {
        return futures.get(task);
    }

    public synchronized ListenableFuture<T> remove(Callable<T> task) {
        ListenableFuture<T> prev = futures.remove(task);
        wake();
        return prev;
    }

    @Override
    protected void startTasks() throws Exception {
        List<Callable<T>> items;
        synchronized (this) {
            items = ImmutableList.copyOf(this.tasks);
        }
        synchronized (futures) {
            for (Callable<T> item : items) {
                assert !(futures.containsKey(item));
                ListenableFuture<T> future = execute.submit(item);
                futures.put(item, future);
                Futures.addCallback(future,
                        new CallableMonitorListener(future), listenerExecute);
            }
        }
    }

    @Override
    protected void stopTasks() throws Exception {
        List<ListenableFuture<T>> items;
        synchronized (this) {
            items = ImmutableList.copyOf(this.futures.values());
        }
        for (ListenableFuture<T> item : items) {
            if (!(item.isDone() || item.isCancelled())) {
                item.cancel(true);
            }
        }
    }

    @Override
    protected boolean monitorTasks() throws Exception {
        boolean allTerminated = true;
        List<ListenableFuture<T>> futures;
        synchronized (this) {
            futures = ImmutableList.copyOf(this.futures.values());
        }
        for (ListenableFuture<T> item : futures) {
            if (!(item.isCancelled() || item.isDone())) {
                allTerminated = allTerminated && false;
            } else {
                try {
                    item.get();
                } catch (Exception e) {
                    throw new TaskMonitorException(e);
                }
            }
        }
        return !allTerminated;
    }

    @Override
    protected Executor executor() {
        return this.execute;
    }
}
