package org.apache.zookeeper.util;

import java.util.concurrent.atomic.AtomicReference;


import com.google.inject.Inject;

public class EventfulReference<T> extends ForwardingEventful implements Eventful {

    public static <T> EventfulReference<T> create(Eventful eventful) {
        return new EventfulReference<T>(eventful);
    }

    public static <T> EventfulReference<T> create(Eventful eventful, T state) {
        return new EventfulReference<T>(eventful, state);
    }

    protected final AtomicReference<T> state;

    @Inject
    protected EventfulReference(Eventful eventful) {
        this(eventful, new AtomicReference<T>());
    }    
    
    protected EventfulReference(Eventful eventful, T state) {
        this(eventful, new AtomicReference<T>(state));
    }

    protected EventfulReference(Eventful eventful, AtomicReference<T> state) {
    	super(eventful);
        this.state = state;
    }
    
    public T get() {
        return state.get();
    }
    
    public AtomicReference<T> state() {
        return state;
    }
    
    public boolean set(T nextState) {
        return (getAndSet(nextState) != null);
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
        if (updated && (prevState != nextState)) {
            post(nextState);
        }
        return updated;
    }
}
