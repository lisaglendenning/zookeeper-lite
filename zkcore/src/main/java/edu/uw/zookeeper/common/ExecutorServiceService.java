package edu.uw.zookeeper.common;

import static com.google.common.base.Preconditions.*;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

public class ExecutorServiceService<T extends ExecutorService> extends AbstractIdleService implements Reference<T> {

    public static <T extends ExecutorService> ExecutorServiceService<T> newInstance(
            T instance) {
        return newInstance(instance, Optional.<Executor>absent(), DEFAULT_WAIT_INTERVAL);
    }

    public static <T extends ExecutorService> ExecutorServiceService<T> newInstance(
            T instance,
            Optional<Executor> thisExecutor,
            TimeValue waitInterval) {
        return new ExecutorServiceService<T>(instance, thisExecutor, waitInterval);
    }
    
    protected static final TimeValue DEFAULT_WAIT_INTERVAL = TimeValue.create(30L, TimeUnit.SECONDS);
    
    private final Logger logger = LogManager
            .getLogger(getClass());
    private final TimeValue waitInterval;
    private final Optional<Executor> thisExecutor;
    private final T instance;

    protected ExecutorServiceService(
            T instance,
            Optional<Executor> thisExecutor,
            TimeValue waitInterval) {
        this.instance = checkNotNull(instance);
        this.thisExecutor = checkNotNull(thisExecutor);
        this.waitInterval = checkNotNull(waitInterval);
    }
    
    @Override
    public T get() {
        return instance;
    }

    @Override
    protected Executor executor() {
        if (thisExecutor.isPresent()) {
            return thisExecutor.get();
        } else {
            return super.executor();
        }
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        instance.shutdown();
        if (! instance.awaitTermination(waitInterval.value(), waitInterval.unit())) {
            logger.debug("shutdownNow: {}", instance);
            List<Runnable> canceled = instance.shutdownNow();
            if (! canceled.isEmpty()) {
                logger.debug("Canceled: {}", canceled);
            }
        }
    }
}
