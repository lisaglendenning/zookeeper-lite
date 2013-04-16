package edu.uw.zookeeper.util;

public abstract class ForwardingEventful implements Eventful {

    private final Eventful delegate;

    protected ForwardingEventful(Eventful eventful) {
        this.delegate = eventful;
    }

    @Override
    public void post(Object event) {
        delegate.post(event);
    }

    @Override
    public void register(Object handler) {
        delegate.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate.unregister(handler);
    }
}
