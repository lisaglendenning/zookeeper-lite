package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;


public class EventfulAtomicUpdater<T> extends ForwardingEventful implements Eventful, AtomicUpdater<T> {

    public static <T> EventfulAtomicUpdater<T> create(Eventful eventful, AtomicUpdater<T> state) {
        return new EventfulAtomicUpdater<T>(eventful, state);
    }

    protected final AtomicUpdater<T> state;

    protected EventfulAtomicUpdater(Eventful eventful, AtomicUpdater<T> state) {
    	super(eventful);
        this.state = checkNotNull(state);
    }
    
    public T get() {
        return state.get();
    }
    
    protected AtomicUpdater<T> state() {
        return state;
    }
    
    public boolean set(T nextState) {
        return (getAndSet(nextState) != null);
    }

    public T getAndSet(T nextState) {
        T prevState = state.getAndSet(checkNotNull(nextState));
        if ((prevState != null) && (prevState != nextState)) {
            post(nextState);
        }
        return prevState;
    }
    
    public boolean compareAndSet(T prevState,
            T nextState) {
        boolean updated = state.compareAndSet(checkNotNull(prevState),
        		checkNotNull(nextState));
        if (updated && (prevState != nextState)) {
            post(nextState);
        }
        return updated;
    }
}
