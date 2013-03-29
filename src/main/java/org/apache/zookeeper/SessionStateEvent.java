package org.apache.zookeeper;


public class SessionStateEvent extends SessionEventValue<Session.State> {
    
    public static SessionStateEvent create(Session session, Session.State state) {
        return new SessionStateEvent(session, state);
    }

    public SessionStateEvent(Session session, Session.State state) {
        super(session, state);
    }
}
