package edu.uw.zookeeper.server;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.util.Eventful;

public class ExpiringSessionManager extends SessionManager {

    public static ExpiringSessionManager create(Eventful eventful,
            SessionParametersPolicy policy) {
        return new ExpiringSessionManager(eventful, policy);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(ExpiringSessionManager.class);
    protected final Map<Long, Long> touches;

    @Inject
    protected ExpiringSessionManager(Eventful eventful,
            SessionParametersPolicy policy) {
        this(eventful, policy, Collections.synchronizedMap(Maps
                .<Long, Long> newHashMap()));
    }

    protected ExpiringSessionManager(Eventful eventful,
            SessionParametersPolicy policy, Map<Long, Long> touches) {
        super(eventful, policy);
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
            synchronized (sessions) {
                if (sessions.containsKey(sessionId)) {
                    touches.put(sessionId, timestamp);
                    return true;
                }
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
        Set<Long> expired = Sets.newHashSet();
        synchronized (sessions) {
            for (Map.Entry<Long, Session> entry : sessions.entrySet()) {
                long id = entry.getKey();
                if (!touches.containsKey(id)) {
                    continue;
                }
                Session session = entry.getValue();
                long touch = touches.get(entry.getKey());
                long timeOut = timestampUnit.convert(session.parameters()
                        .timeOut().value(), session.parameters().timeOut().unit());
                assert (timeOut != Session.Parameters.NEVER_TIMEOUT);
                long expires = touch + timeOut;
                if (expires < timestamp) {
                    expired.add(id);
                }
            }
        }
        for (long id : expired) {
            expire(id);
        }
    }

    protected long timestamp() {
        return System.currentTimeMillis();
    }

    protected TimeUnit timestampUnit() {
        return TimeUnit.MILLISECONDS;
    }

    @Subscribe
    public void handleSessionStateEvent(SessionStateEvent event) {
        Session session = event.session();
        switch (event.event()) {
        case SESSION_OPENED:
            touch(session.id());
            break;
        case SESSION_CLOSED:
            touches.remove(session.id());
            break;
        default:
            break;
        }
    }
}