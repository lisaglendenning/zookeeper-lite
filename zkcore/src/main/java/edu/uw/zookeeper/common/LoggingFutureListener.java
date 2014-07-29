package edu.uw.zookeeper.common;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.ToStringListenableFuture.SimpleToStringListenableFuture;

public class LoggingFutureListener<V> extends SimpleToStringListenableFuture<V> implements Runnable {

    public static <V, T extends ListenableFuture<V>> T listen(
            Logger logger,
            T future) {
        LoggingFutureListener<V> listener = create(logger, future);
        future.addListener(listener, SameThreadExecutor.getInstance());
        return future;
    }
    
    public static <V> LoggingFutureListener<V> create(
            Logger logger,
            ListenableFuture<V> future) {
        return new LoggingFutureListener<V>(logger, future);
    }

    protected final Logger logger;
    
    protected LoggingFutureListener(
            Logger logger,
            ListenableFuture<V> delegate) {
        super(delegate);
        this.logger = logger;
    }
    
    public Logger logger() {
        return logger;
    }

    @Override
    public void run() {
        if (isDone()) {
            logger().trace("DONE {}", this);
        }
    }

    @Override
    public String toString() {
        return is3rdParty(delegate()) ? super.toString() : delegate().toString();
    }
    
    @Override
    protected Objects.ToStringHelper toStringHelper() {
        return toStringHelper(Objects.toStringHelper(delegate()));
    }
}
