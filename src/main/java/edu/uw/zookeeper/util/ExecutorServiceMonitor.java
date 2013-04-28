package edu.uw.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

public class ExecutorServiceMonitor<T extends ExecutorService> extends AbstractIdleService implements Factory<T> {

    public static <T extends ExecutorService> ExecutorServiceMonitor<T> newInstance(T executorService) {
        return newInstance(executorService, Optional.<Executor>absent(), DEFAULT_WAIT_INTERVAL);
    }

    public static <T extends ExecutorService> ExecutorServiceMonitor<T> newInstance(T executorService,
            Optional<Executor> thisExecutor,
            TimeValue waitInterval) {
        return new ExecutorServiceMonitor<T>(executorService, thisExecutor, waitInterval);
    }
    private static final TimeValue DEFAULT_WAIT_INTERVAL = TimeValue.create(30L, TimeUnit.SECONDS);
    
    private final Logger logger = LoggerFactory
            .getLogger(ExecutorServiceMonitor.class);
    private final TimeValue waitInterval;
    private final Optional<Executor> thisExecutor;
    private final T executorService;

    protected ExecutorServiceMonitor(T executorService,
            Optional<Executor> thisExecutor,
            TimeValue waitInterval) {
        this.executorService = checkNotNull(executorService);
        this.thisExecutor = thisExecutor;
        this.waitInterval = waitInterval;
    }
    
    @Override
    protected Executor executor() {
        if (thisExecutor.isPresent()) {
            return thisExecutor.get();
        } else {
            return super.executor();
        }
    }

    public T get() {
        return executorService;
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        get().shutdown();
        if (! get().awaitTermination(waitInterval.value(), waitInterval.unit())) {
            logger.debug("shutdownNow: {}", get());
            List<Runnable> canceled = get().shutdownNow();
            if (! canceled.isEmpty()) {
                logger.debug("Canceled {}", canceled);
            }
        }
    }
}
