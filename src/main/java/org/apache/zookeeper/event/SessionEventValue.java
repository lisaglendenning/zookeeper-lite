package org.apache.zookeeper.event;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.util.Pair;

import com.google.common.base.Objects;


public class SessionEventValue<T> extends Pair<Session, T> implements SessionEvent {

    public static <T> SessionEventValue<T> create(Session session, T event) {
        return new SessionEventValue<T>(session, event);
    }
    
    public SessionEventValue(Session session, T event) {
        super(session, event);
    }

    @Override
    public Session session() {
        return first();
    }

    public T event() {
        return second();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("session", session())
                .add("event", event())
                .toString();
    }
}
