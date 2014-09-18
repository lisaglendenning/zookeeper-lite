package edu.uw.zookeeper.common;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class LoggingFutureListener<T extends ListenableFuture<?>> implements Runnable {

    public static <T extends ListenableFuture<?>> T listen(
            Logger logger,
            T future) {
        LoggingFutureListener<T> listener = create(logger, future);
        future.addListener(listener, MoreExecutors.directExecutor());
        return future;
    }
    
    public static <T extends ListenableFuture<?>> LoggingFutureListener<T> create(
            Logger logger,
            T future) {
        return new LoggingFutureListener<T>(logger, future);
    }

    protected final Logger logger;
    protected final T future;
    
    protected LoggingFutureListener(
            Logger logger,
            T future) {
        this.logger = logger;
        this.future = future;
    }
    
    public Logger logger() {
        return logger;
    }

    @Override
    public void run() {
        if (future.isDone()) {
            logger().trace("DONE {}", this);
        }
    }

    @Override
    public String toString() {
        return ToStringListenableFuture.toString3rdParty(future);
    }
}
