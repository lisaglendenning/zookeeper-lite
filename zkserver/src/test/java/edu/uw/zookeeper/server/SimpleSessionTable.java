package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.engio.mbassy.PubSubSupport;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public class SimpleSessionTable extends SessionTableAdapter {

    protected final PubSubSupport<Object> publisher;
    protected final Map<Long, Session> sessions;
    protected final AtomicLong counter;
    protected final TimeValue defaultTimeOut;
    
    public SimpleSessionTable(
            PubSubSupport<Object> publisher,
            Map<Long, Session> sessions,
            TimeValue defaultTimeOut) {
        this.publisher = publisher;
        this.sessions = sessions;
        this.defaultTimeOut = defaultTimeOut;
        this.counter = new AtomicLong(0);
    }
    
    @Override
    public Session validate(Session.Parameters parameters) {
        checkNotNull(parameters);
        long id = newSessionId();
        Session session = Session.create(id, parameters);
        put(session);
        return session;
    }

    @Override
    public Session validate(Session session) {
        checkNotNull(session);
        if (session.initialized()) {
            put(session);
            return session;
        } else {
            return validate(session.parameters());
        }
    }

    @Override
    public Session newSession() {
        long id = newSessionId();
        Session session = Session.create(id, 
                Session.Parameters.create(defaultTimeOut, newPassword()));
        put(session);
        return session;
    }
    
    protected long newSessionId() {
        return counter.incrementAndGet();
    }
    
    protected byte[] newPassword() {
        return Session.Parameters.NO_PASSWORD;
    }

    @Override
    protected Map<Long, Session> sessions() {
        return sessions;
    }

    @Override
    protected PubSubSupport<Object> publisher() {
        return publisher;
    }
}
