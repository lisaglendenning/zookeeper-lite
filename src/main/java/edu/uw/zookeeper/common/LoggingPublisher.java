package edu.uw.zookeeper.common;

import org.apache.logging.log4j.Logger;

public class LoggingPublisher implements Publisher, Reference<Publisher> {

    public static LoggingPublisher create(
            Logger logger,
            Publisher delegate,
            Object self) {
        return new LoggingPublisher(logger, delegate, self);
    }

    protected final Logger logger;
    protected final Publisher delegate;
    protected final Object self;
    
    public LoggingPublisher(Logger logger, Publisher delegate, Object self) {
        this.logger = logger;
        this.delegate = delegate;
        this.self = self;
    }
    
    public Logger getLogger() {
        return logger;
    }
    
    public Object getSelf() {
        return self;
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
        getLogger().entry(event, self);
        get().post(event);
        getLogger().exit();
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
