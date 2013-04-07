package org.apache.zookeeper.event;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.data.Operation;

public class SessionRequestEvent extends SessionEventValue<Operation.Request> {

    public static SessionRequestEvent create(Session session,
            Operation.Request state) {
        return new SessionRequestEvent(session, state);
    }

    public SessionRequestEvent(Session session, Operation.Request state) {
        super(session, state);
    }
}
