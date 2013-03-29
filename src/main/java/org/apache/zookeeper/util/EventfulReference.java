package org.apache.zookeeper.util;

import java.util.concurrent.atomic.AtomicReference;


import com.google.inject.Inject;

public class EventfulReference<T> implements Eventful {

    public static <T> EventfulReference<T> create(Eventful eventful) {
        return new EventfulReference<T>(eventful);
    }

    public static <T> EventfulReference<T> create(Eventful eventful, T state) {
        return new EventfulReference<T>(eventful, state);
    }

    protected final Eventful eventful;
    protected final AtomicReference<T> state;

    @Inject
    protected EventfulReference(Eventful eventful) {
        this(eventful, new AtomicReference<T>());
    }    
    
    protected EventfulReference(Eventful eventful, T state) {
        this(eventful, new AtomicReference<T>(state));
    }

    protected EventfulReference(Eventful eventful, AtomicReference<T> state) {
        this.state = state;
        this.eventful = eventful;
    }
    
    public T get() {
        return state.get();
    }
    
    public AtomicReference<T> state() {
        return state;
    }
    
    public Eventful eventful() {
        return eventful;
    }

    public void set(T nextState) {
        getAndSet(nextState);
    }

    public T getAndSet(T nextState) {
        T prevState = state.getAndSet(nextState);
        if (prevState != nextState) {
            post(nextState);
        }
        return prevState;
    }
    
    public boolean compareAndSet(T prevState,
            T nextState) {
        boolean updated = state.compareAndSet(prevState, nextState);
        if (updated) {
            post(nextState);
        }
        return updated;
    }

    @Override
    public void post(Object event) {
        eventful.post(event);
    }

    @Override
    public void register(Object handler) {
        eventful.register(handler);
    }

    @Override
    public void unregister(Object handler) {
        eventful.unregister(handler);
    }
}
