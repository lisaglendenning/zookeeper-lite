package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.Session;

public class SessionStateEvent extends SessionEventValue<Session.State> {

    public static SessionStateEvent create(Session session, Session.State event) {
        return new SessionStateEvent(session, event);
    }

    private SessionStateEvent(Session session, Session.State event) {
        super(session, event);
    }
}
