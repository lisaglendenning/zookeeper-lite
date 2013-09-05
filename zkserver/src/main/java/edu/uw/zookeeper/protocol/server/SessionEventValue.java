package edu.uw.zookeeper.protocol.server;


import com.google.common.base.Objects;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.Session;

public class SessionEventValue<T> extends AbstractPair<Session, T> implements
        SessionEvent {

    public static <T> SessionEventValue<T> create(Session session, T event) {
        return new SessionEventValue<T>(session, event);
    }

    protected SessionEventValue(Session session, T event) {
        super(session, event);
    }

    @Override
    public Session session() {
        return first;
    }

    public T event() {
        return second;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("session", session())
                .add("event", event()).toString();
    }
}
