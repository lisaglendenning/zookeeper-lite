package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.Map;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Objects;

import edu.uw.zookeeper.protocol.Session;
import edu.uw.zookeeper.protocol.server.SessionStateEvent;

public abstract class SessionTableAdapter implements SessionTable {

    protected SessionTableAdapter() {}

    @Override
    public void publish(Object event) {
        publisher().publish(event);
    }

    @Override
    public void subscribe(Object handler) {
        publisher().subscribe(handler);
    }

    @Override
    public boolean unsubscribe(Object handler) {
        return publisher().unsubscribe(handler);
    }

    @Override
    public Iterator<Session> iterator() {
        return sessions().values().iterator();
    }

    @Override
    public Session remove(long id) {
        Session session = sessions().remove(id);
        if (session != null) {
            publish(SessionStateEvent
                    .create(session, Session.State.SESSION_CLOSED));
        }
        return session;
    }

    @Override
    public Session get(long id) {
        return sessions().get(id);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(sessions()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        return Objects.equal(sessions(), ((SessionTableAdapter) obj).sessions());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(sessions());
    }
    
    protected Session put(Session session) {
        Session prev = sessions().put(session.id(), session);
        if (prev == null) {
            publish(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
        }
        return prev;
    }

    protected abstract Map<Long, Session> sessions();
    
    protected abstract PubSubSupport<Object> publisher();
}
