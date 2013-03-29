package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.SessionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;

public class SessionManager implements Eventful {

    public static class SessionManagerModule extends AbstractModule {

        public static SessionManagerModule get() {
            return new SessionManagerModule();
        }
        
        protected SessionManagerModule() {}
    
        @Override
        protected void configure() {
        }
    }
    
    protected final Logger logger = LoggerFactory.getLogger(SessionManager.class);

    protected final Eventful eventful;
    protected final SessionParametersPolicy policy;
    protected final Map<Long, Session> sessions;
    
    @Inject
    protected SessionManager(SessionParametersPolicy policy,
            Eventful eventful) {
        this(policy, eventful,
                Collections.synchronizedMap(Maps.<Long, Session>newHashMap()));
    }
    
    protected SessionManager(
            SessionParametersPolicy policy,
            Eventful eventful,
            Map<Long, Session> sessions) {
        this.policy = policy;
        this.eventful = eventful;
        this.sessions = sessions;
    }

    public synchronized Session add() {
        long id = policy.newSessionId();
        long timeOut = policy.maxTimeout();
        byte[] passwd = policy.newPassword(id);
        TimeUnit timeOutUnit = policy.timeoutUnit();
        SessionParameters parameters = SessionParameters.create(timeOut, passwd, timeOutUnit);
        return newSession(id, parameters);
    }
    
    public synchronized Session add(long id, SessionParameters parameters) {
        checkNotNull(parameters);
        Session session = sessions.get(id);
        if (session == null) {
            checkArgument(id == Session.UNINITIALIZED_ID);
            session = add(parameters);
        } else {
            checkArgument(policy.validatePassword(id, parameters.password()));
        }
        return session;
    }

    public synchronized Session add(SessionParameters parameters) {
        checkNotNull(parameters);
        Session session = newSession(parameters);
        return session;
    }
    
    public synchronized Session get(long id) {
        return sessions.get(id);
    }
    
    public synchronized Session remove(long id) {
        Session session = sessions.remove(id);
        if (logger.isDebugEnabled()) {
            if (session != null) {
                logger.debug("Removed session: {}", session);
            }
            post(SessionStateEvent.create(session, Session.State.CLOSED));
        }
        return session;
    }
    
    protected synchronized Session newSession(SessionParameters parameters) {
        long id = policy.newSessionId();
        TimeUnit timeOutUnit = parameters.timeOutUnit();
        long timeOut = policy.boundTimeout(parameters.timeOut(),
                timeOutUnit);
        byte[] passwd = policy.newPassword(id);
        parameters = SessionParameters.create(timeOut, passwd, timeOutUnit);
        return newSession(id, parameters);
    }

    protected synchronized Session newSession(long id, SessionParameters parameters) {
        assert (! sessions.containsKey(id));
        Session session = Session.create(id, parameters);
        sessions.put(id, session);
        if (logger.isDebugEnabled()) {
            logger.debug("Added session: {}", session);
        }
        post(SessionStateEvent.create(session, Session.State.OPENED));
        return session;
    }

    @Override
    public void post(Object event) {
        eventful.post(event);
    }

    @Override
    public void register(Object object) {
        eventful.register(object);
    }

    @Override
    public void unregister(Object object) {
        eventful.unregister(object);
    }
}
