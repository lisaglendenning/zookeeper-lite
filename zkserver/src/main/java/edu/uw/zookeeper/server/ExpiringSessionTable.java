package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.protocol.server.SessionStateEvent;

public class ExpiringSessionTable extends ConcurrentSessionTable {

    public static ExpiringSessionTable newInstance(Publisher publisher,
            SessionParametersPolicy policy) {
        return new ExpiringSessionTable(publisher, policy);
    }

    protected final ConcurrentMap<Long, Long> touches;

    protected ExpiringSessionTable(Publisher publisher,
            SessionParametersPolicy policy) {
        this(publisher, policy, Maps.<Long, Long> newConcurrentMap());
    }

    protected ExpiringSessionTable(Publisher publisher,
            SessionParametersPolicy policy, ConcurrentMap<Long, Long> touches) {
        super(publisher, policy);
        this.touches = touches;
        register(this);
    }

    public boolean touch(long id) {
        Session session = get(id);
        if (session != null) {
            return touch(session);
        }
        return false;
    }

    protected boolean touch(Session session) {
        return touch(session, timestamp());
    }

    protected boolean touch(Session session, long timestamp) {
        long sessionId = session.id();
        if (session.parameters().timeOut().value() != Session.Parameters.NEVER_TIMEOUT) {
            if (sessions.containsKey(sessionId)) {
                touches.put(sessionId, timestamp);
                return true;
            }
        }
        return false;
    }

    public void expire(long id) {
        // we won't remove the session ourselves
        // we'll just let everyone know that it's expired and let them
        // decide to remove it
        Session session = get(id);
        if (session != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Expiring session {}", session);
            }
            post(SessionStateEvent.create(session,
                    Session.State.SESSION_EXPIRED));
        }
    }

    public void triggerExpired() {
        long timestamp = timestamp();
        TimeUnit timestampUnit = timestampUnit();
        Iterator<Map.Entry<Long, Long>> itr = touches.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Long, Long> entry = itr.next();
            Long id = entry.getKey();
            Long touch = entry.getValue();
            Session session = sessions.get(id);
            if (session != null && session.parameters().timeOut().value() != Session.Parameters.NEVER_TIMEOUT) {
                long timeOut = timestampUnit.convert(session.parameters()
                        .timeOut().value(), session.parameters().timeOut().unit());
                long expires = touch + timeOut;
                if (expires < timestamp) {
                    expire(id);
                    // touch this entry so that we don't try to expire it again right away
                    touches.replace(id, timestamp);
                }
            } else {
                itr.remove();
            }
        }
    }
    
    @Override
    public Session newSession() {
        Session session = super.newSession();
        touch(session);
        return session;
    }

    @Override
    public Session validate(Session session) {
        session = super.validate(session);
        touch(session);
        return session;
    }

    @Override
    public Session validate(Session.Parameters parameters) {
        Session session = super.validate(parameters);
        touch(session);
        return session;
    }
    
    @Override
    public Session remove(long id) {
        touches.remove(id);
        return super.remove(id);
    }

    protected long timestamp() {
        return System.currentTimeMillis();
    }

    protected TimeUnit timestampUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
