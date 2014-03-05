package edu.uw.zookeeper.common;

import net.engio.mbassy.bus.BusRuntime;
import net.engio.mbassy.bus.PubSubSupport;

import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;

public class LoggingPublisher<T> implements PubSubSupport<T>, Reference<PubSubSupport<T>> {

    public static <T>LoggingPublisher<T> create(
            Logger logger,
            PubSubSupport<T> delegate,
            Object...params) {
        return new LoggingPublisher<T>(logger, delegate, params);
    }

    private final Logger logger;
    private final PubSubSupport<T> delegate;
    private final Object[] params;
    
    public LoggingPublisher(Logger logger, PubSubSupport<T> delegate, Object...params) {
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
    public PubSubSupport<T> get() {
        return delegate;
    }
    
    @Override
    public void subscribe(Object listener) {
        delegate.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(Object listener) {
        return delegate.unsubscribe(listener);
    }

    @Override
    public void publish(T event) {
        if (logger.isTraceEnabled()) {
            int nparams = this.params.length + 1;
            Object[] params = new Object[nparams];
            params[0] = event;
            for (int i=1; i<nparams; ++i) {
                params[i] = this.params[i-1];
            }
            logger.entry(params);
        }
        delegate.publish(event);
        if (logger.isTraceEnabled()) {
            logger.exit();
        }
    }
    
    @Override
    public BusRuntime getRuntime() {
        return delegate.getRuntime();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
