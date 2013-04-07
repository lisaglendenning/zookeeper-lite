package org.apache.zookeeper.event;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.data.Operation;

public class SessionResponseEvent extends SessionEventValue<Operation.Response> {

    public static SessionResponseEvent create(Session session,
            Operation.Response state) {
        return new SessionResponseEvent(session, state);
    }

    public SessionResponseEvent(Session session, Operation.Response state) {
        super(session, state);
    }
}
