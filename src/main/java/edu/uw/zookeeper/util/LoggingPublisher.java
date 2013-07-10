package edu.uw.zookeeper.util;

import org.slf4j.Logger;

import com.google.common.base.Function;

public class LoggingPublisher implements Publisher, Reference<Publisher> {

    protected final Logger logger;
    protected final Publisher delegate;
    protected final Function<Object, String> message;
    
    public LoggingPublisher(Logger logger, Publisher delegate, Function<Object, String> message) {
        this.logger = logger;
        this.delegate = delegate;
        this.message = message;
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
        if (logger.isTraceEnabled()) {
            logger.trace(message.apply(event));
        }
        get().post(event);
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
