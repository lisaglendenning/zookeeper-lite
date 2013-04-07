package org.apache.zookeeper.event;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionConnection;
import org.apache.zookeeper.SessionConnection.State;

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
