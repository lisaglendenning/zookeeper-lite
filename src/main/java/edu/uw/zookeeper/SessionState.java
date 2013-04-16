package edu.uw.zookeeper;


import com.google.inject.Inject;

import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.EventfulAtomicUpdater;
import edu.uw.zookeeper.util.EventfulAutomataState;

public class SessionState extends EventfulAutomataState<Session.State> {

    public static SessionState create(Eventful eventful) {
        return new SessionState(eventful);
    }

    public static SessionState create(Eventful eventful, Session.State state) {
        return new SessionState(eventful, state);
    }

    @Inject
    protected SessionState(Eventful eventful) {
        this(eventful, Session.State.SESSION_UNINITIALIZED);
    }

    protected SessionState(Eventful eventful, Session.State state) {
        super(eventful, state);
        eventful.post(state);
    }
}
