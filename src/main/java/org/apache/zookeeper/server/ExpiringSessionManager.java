package org.apache.zookeeper.server;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;

public class ExpiringSessionManager extends SessionManager {

    public static class SessionManagerModule extends AbstractModule {

        public static SessionManagerModule get() {
            return new SessionManagerModule();
        }
        
        protected SessionManagerModule() {}
    
        @Override
        protected void configure() {
            bind(SessionManager.class).to(ExpiringSessionManager.class);
        }
    }
    
    protected final Logger logger = LoggerFactory.getLogger(ExpiringSessionManager.class);

    protected final Map<Long, Long> touches;
    
    @Inject
    protected ExpiringSessionManager(
            SessionParametersPolicy policy,
            Eventful eventful) {
        this(policy, eventful,
                Collections.synchronizedMap(Maps.<Long, Long>newHashMap()));
    }

    protected ExpiringSessionManager(
            SessionParametersPolicy policy,
            Eventful eventful,
            Map<Long, Long> touches) {
        super(policy, eventful);
        this.touches = touches;
    }
    
    public synchronized Session add() {
        Session session = super.add();
        touch(session.id());
        return session;
    }
    
    public synchronized Session add(long id, SessionParameters parameters) {
        Session session = super.add(id, parameters);
        touch(session);
        return session;
    }

    public synchronized Session add(SessionParameters parameters) {
        Session session = super.add(parameters);
        touch(session);
        return session;
    }
    
    public synchronized Session remove(long id) {
        Session session = super.remove(id);
        touches.remove(id);
        return session;
    }
    
    public synchronized boolean touch(long id) {
        Session session = sessions.get(id);
        if (session != null) {
            return touch(sessions.get(id));
        }
        return false;
    }

    protected synchronized boolean touch(Session session) {
        return touch(session, timestamp());
    }
    
    protected synchronized boolean touch(Session session, long timestamp) {
        if (session.parameters().timeOut() != SessionParameters.NEVER_TIMEOUT) {
            touches.put(session.id(), timestamp);
            return true;
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
            post(SessionStateEvent.create(session, Session.State.EXPIRED));
        }
    }
    
    public void triggerExpired() {
        long timestamp = timestamp();
        TimeUnit timestampUnit = timestampUnit();
        Set<Long> expired = Sets.newHashSet();
        synchronized (this) {
            for (Map.Entry<Long, Session> entry: sessions.entrySet()) {
                long id = entry.getKey();
                if (! touches.containsKey(id)) {
                    continue;
                }
                Session session = entry.getValue();
                long touch = touches.get(entry.getKey());
                long timeOut = timestampUnit.convert(session.parameters().timeOut(), 
                        session.parameters().timeOutUnit());
                assert (timeOut != SessionParameters.NEVER_TIMEOUT);
                long expires = touch + timeOut;
                if (expires < timestamp) {
                    expired.add(id);
                }
            }
        }
        for (long id: expired) {
            expire(id);
        }
    }
    
    protected long timestamp() {
        return System.currentTimeMillis();
    }

    protected TimeUnit timestampUnit() {
        return TimeUnit.MILLISECONDS;
    }
}
