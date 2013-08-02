package edu.uw.zookeeper.util;

import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;

public class LoggingPublisher implements Publisher, Reference<Publisher> {

    public static LoggingPublisher create(
            Logger logger, 
            Function<Object, String> message,
            Publisher delegate) {
        return new LoggingPublisher(Logging.create(logger, message), delegate);
    }

    public static class Logging extends Factories.Holder<Logger> {

        public static Logging create(
                Logger logger,
                Function<Object, String> message) {
            return new Logging(logger, message);
        }

        protected final Function<Object, String> message;
        
        public Logging(
                Logger logger,
                Function<Object, String> message) {
            super(logger);
            this.message = message;
        }

        public void log(Object event) {
            if (get().isTraceEnabled()) {
                get().trace(message.apply(event));
            }
        }
    }

    protected final Logging logging;
    protected final Publisher delegate;
    
    public LoggingPublisher(Logging logging, Publisher delegate) {
        this.logging = logging;
        this.delegate = delegate;
    }
    
    public Logging getLogging() {
        return logging;
    }
    
    @Override
    public Publisher get() {
        return delegate;
    }
    
    @Override
    public void register(Object handler) {
        get().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        get().unregister(handler);
    }

    @Override
    public void post(Object event) {
        getLogging().log(event);
        get().post(event);
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
