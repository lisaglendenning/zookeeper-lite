package edu.uw.zookeeper.event;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionConnection;
import edu.uw.zookeeper.SessionConnection.State;

public class SessionConnectionStateEvent extends
        SessionEventValue<SessionConnection.State> {

    public static SessionConnectionStateEvent create(Session session,
            SessionConnection.State state) {
        return new SessionConnectionStateEvent(session, state);
    }

    public SessionConnectionStateEvent(Session session,
            SessionConnection.State state) {
        super(session, state);
    }
}
