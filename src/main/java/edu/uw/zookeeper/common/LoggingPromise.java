package edu.uw.zookeeper.common;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;

public class LoggingPromise<V> extends ForwardingPromise<V> {

    public static <V> LoggingPromise<V> create(
            Logger logger,
            Promise<V> delegate) {
        return new LoggingPromise<V>(Logging.create(logger), delegate);
    }
    
    public static <V> LoggingPromise<V> create(
            Logging logging,
            Promise<V> delegate) {
        return new LoggingPromise<V>(logging, delegate);
    }
    
    public static class Logging extends Factories.Holder<Logger> {
        
        public static Logging create(Logger logger) {
            return new Logging(logger);
        }
        
        public Logging(Logger logger) {
            super(logger);
        }

        public void log(String message, Object self) {
            get().trace("{}: {}", message, self);
        }
    }
    
    private final Logging logging;
    private final Promise<V> delegate;
    
    public LoggingPromise(
            Logging logging,
            Promise<V> delegate) {
        this.logging = logging;
        this.delegate = delegate;
    }

    public Logging getLogging() {
        return logging;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean isCancelled = super.cancel(mayInterruptIfRunning);
        if (isCancelled) {
            getLogging().log("CANCELLED", this);
        }
        return isCancelled;
    }
    
    @Override
    public boolean set(V value) {
        boolean isSet = super.set(value);
        if (isSet) {
            getLogging().log("SET", this);
        }
        return isSet;
    }

    @Override
    public boolean setException(Throwable throwable) {
        boolean isSet = super.setException(throwable);
        if (isSet) {
            getLogging().log("EXCEPTION", this);
        }
        return isSet;
    }
    
    @Override
    public String toString() {
        Objects.ToStringHelper toString = Objects.toStringHelper(delegate());
        if (isDone()) {
            Object value;
            if (isCancelled()) {
                value = "cancelled";
            } else {
                try {
                    value = get();
                } catch (Throwable t) {
                    value = t;
                }
            }
            toString.addValue(value);
        }
        return toString.toString();
    }

    @Override
    protected Promise<V> delegate() {
        return delegate;
    }
}
