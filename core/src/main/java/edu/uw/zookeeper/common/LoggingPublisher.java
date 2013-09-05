package edu.uw.zookeeper.common;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

public class LoggingPublisher implements Publisher, Reference<Publisher> {

    public static LoggingPublisher create(
            Logger logger,
            Publisher delegate,
            Object...params) {
        return new LoggingPublisher(logger, delegate, params);
    }

    private final Logger logger;
    private final Publisher delegate;
    private final Object[] params;
    
    public LoggingPublisher(Logger logger, Publisher delegate, Object...params) {
        this.logger = logger;
        this.delegate = delegate;
        this.params = params;
    }
    
    public Logger getLogger() {
        return logger;
    }
    
    public ImmutableList<Object> getParams() {
        return ImmutableList.copyOf(params);
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
            int nparams = this.params.length + 1;
            Object[] params = new Object[nparams];
            params[0] = event;
            for (int i=1; i<nparams; ++i) {
                params[i] = this.params[i-1];
            }
            getLogger().entry(params);
        }
        get().post(event);
        if (logger.isTraceEnabled()) {
            getLogger().exit();
        }
    }
    
    @Override
    public String toString() {
        return get().toString();
    }
}
