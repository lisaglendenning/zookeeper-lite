package org.apache.zookeeper;

import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.EventfulAutomataState;

import com.google.inject.Inject;

public class SessionConnectionState extends EventfulAutomataState<SessionConnection.State> {
    
    public static SessionConnectionState create(Eventful eventful) {
        return new SessionConnectionState(eventful);
    }

    public static SessionConnectionState create(Eventful eventful, SessionConnection.State state) {
        return new SessionConnectionState(eventful, state);
    }

    @Inject
    protected SessionConnectionState(Eventful eventful) {
        this(eventful, SessionConnection.State.ANONYMOUS);
    }

    protected SessionConnectionState(Eventful eventful, SessionConnection.State state) {
        super(eventful, state);
        eventful.post(state);
    }
}
