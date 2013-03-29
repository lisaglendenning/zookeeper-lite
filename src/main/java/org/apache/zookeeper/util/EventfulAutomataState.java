package org.apache.zookeeper.util;

import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulReference;


public class EventfulAutomataState<T extends AutomataState<T>> extends EventfulReference<T> {
    
    public static <T extends AutomataState<T>> EventfulAutomataState<T> create(Eventful eventful, T state) {
        return new EventfulAutomataState<T>(eventful, state);
    }

    protected EventfulAutomataState(Eventful eventful, T state) {
        super(eventful, state);
        eventful.post(state);
    }

    @Override
    public T getAndSet(T nextState) {
        T prevState = state.get();
        if (! compareAndSet(prevState, nextState)) {
            throw new IllegalStateException();
        }
        return prevState;
    }
    
    @Override
    public boolean compareAndSet(T prevState, T nextState) {
        if (! prevState.validTransition(nextState)) {
            throw new IllegalStateException();
        }
        return super.compareAndSet(prevState, nextState);
    }
}
