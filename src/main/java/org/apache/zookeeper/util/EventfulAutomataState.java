package org.apache.zookeeper.util;

import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulAtomicUpdater;


public class EventfulAutomataState<T extends AutomataState<T>> extends EventfulAtomicUpdater<T> {
    
    public static <T extends AutomataState<T>> EventfulAutomataState<T> create(Eventful eventful, T state) {
        return new EventfulAutomataState<T>(eventful, state);
    }

    protected EventfulAutomataState(Eventful eventful, T state) {
        super(eventful, AutomataState.Reference.create(state));
        eventful.post(state);
    }
}
