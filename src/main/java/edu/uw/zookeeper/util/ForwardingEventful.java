package edu.uw.zookeeper.util;

public abstract class ForwardingEventful implements Eventful {

    private final Publisher publisher;

    protected ForwardingEventful(Publisher publisher) {
        this.publisher = publisher;
    }
    
    protected Publisher publisher() {
        return publisher;
    }

    protected void post(Object event) {
        publisher().post(event);
    }

    @Override
    public void register(Object handler) {
        publisher().register(handler);
    }

    @Override
    public void unregister(Object handler) {
        publisher().unregister(handler);
    }
}
