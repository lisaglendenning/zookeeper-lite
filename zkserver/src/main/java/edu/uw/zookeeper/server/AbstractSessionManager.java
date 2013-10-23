package edu.uw.zookeeper.server;

import org.apache.logging.log4j.Logger;

import net.engio.mbassy.PubSubSupport;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.protocol.Session;

public abstract class AbstractSessionManager implements SessionManager {

    protected AbstractSessionManager() {}

    @Override
    public void publish(SessionEvent event) {
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
    public Session remove(long id) {
        Session session = doRemove(id);
        if (session != null) {
            logger().debug("Removed session: {}", session);
            publish(SessionStateEvent
                    .create(session, Session.State.SESSION_CLOSED));
        }
        return session;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(Iterators.toString(iterator())).toString();
    }

    protected abstract Session doRemove(long id);

    protected abstract Logger logger();
    
    protected abstract PubSubSupport<? super SessionEvent> publisher();
}
