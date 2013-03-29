package org.apache.zookeeper;

import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulAutomataState;
import org.apache.zookeeper.util.EventfulReference;

import com.google.inject.Inject;

public class SessionState extends EventfulAutomataState<Session.State> {
    
    public static SessionState create(Eventful eventful) {
        return new SessionState(eventful);
    }

    public static SessionState create(Eventful eventful, Session.State state) {
        return new SessionState(eventful, state);
    }

    @Inject
    protected SessionState(Eventful eventful) {
        this(eventful, Session.State.OPENED);
    }

    protected SessionState(Eventful eventful, Session.State state) {
        super(eventful, state);
        eventful.post(state);
    }
}
