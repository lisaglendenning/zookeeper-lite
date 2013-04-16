package edu.uw.zookeeper.util;

public class ForwardingEventful implements Eventful {

    protected final Eventful delegate;

    public ForwardingEventful(Eventful eventful) {
        this.delegate = eventful;
    }

    @Override
    public void post(Object event) {
        delegate().post(event);
    }

    @Override
    public void register(Object handler) {
        delegate().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        delegate().unregister(handler);
    }

    protected Eventful delegate() {
        return delegate;
    }
}
