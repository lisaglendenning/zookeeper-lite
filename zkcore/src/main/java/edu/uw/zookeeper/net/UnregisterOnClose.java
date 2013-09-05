package edu.uw.zookeeper.net;

import com.google.common.eventbus.Subscribe;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Pair;

public class UnregisterOnClose extends Pair<Object, Eventful> {

    public static UnregisterOnClose create(Object first, Eventful second) {
        return new UnregisterOnClose(first, second);
    }
    
    public UnregisterOnClose(Object first, Eventful second) {
        super(first, second);
        second.register(first);
        second.register(this);
    }

    @Subscribe
    public void handleTransition(Automaton.Transition<?> event) {
        if (Connection.State.CONNECTION_CLOSED == event.to()) {
            try {
                second().unregister(this);
            } catch (IllegalArgumentException e) {}
            try {
                second().unregister(first());
            } catch (IllegalArgumentException e) {}
        }
    }
}
