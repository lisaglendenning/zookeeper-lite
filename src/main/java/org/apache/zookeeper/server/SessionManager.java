package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.event.SessionStateEvent;
import org.apache.zookeeper.util.Eventful;
import org.apache.zookeeper.util.ForwardingEventful;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;

public class SessionManager extends ForwardingEventful implements
        Iterable<Session> {

    public static SessionManager create(Eventful eventful,
            SessionParametersPolicy policy) {
        return new SessionManager(eventful, policy);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(SessionManager.class);
    protected final SessionParametersPolicy policy;
    protected final Map<Long, Session> sessions;

    @Inject
    protected SessionManager(Eventful eventful, SessionParametersPolicy policy) {
        this(eventful, policy, Collections.synchronizedMap(Maps
                .<Long, Session> newHashMap()));
    }

    protected SessionManager(Eventful eventful, SessionParametersPolicy policy,
            Map<Long, Session> sessions) {
        super(eventful);
        this.policy = policy;
        this.sessions = sessions;
    }

    @Override
    public Iterator<Session> iterator() {
        return sessions.values().iterator();
    }

    public Session add() {
        long id = policy.newSessionId();
        long timeOut = policy.maxTimeout();
        byte[] passwd = policy.newPassword(id);
        TimeUnit timeOutUnit = policy.timeoutUnit();
        SessionParameters parameters = SessionParameters.create(timeOut,
                passwd, timeOutUnit);
        Session session = newSession(id, parameters);
        post(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
        return session;
    }

    public Session add(long id, SessionParameters parameters) {
        checkNotNull(parameters);
        Session session = get(id);
        if (session == null) {
            checkArgument(id == Session.UNINITIALIZED_ID);
            session = add(parameters);
        } else {
            checkArgument(policy.validatePassword(id, parameters.password()));
        }
        return session;
    }

    public Session add(SessionParameters parameters) {
        Session session = newSession(checkNotNull(parameters));
        post(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
        return session;
    }

    public Session get(long id) {
        return sessions.get(id);
    }

    public Session remove(long id) {
        Session session = sessions.remove(id);
        if (session != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Removed session: {}", session);
            }
            post(SessionStateEvent
                    .create(session, Session.State.SESSION_CLOSED));
        }
        return session;
    }

    protected Session newSession(SessionParameters parameters) {
        long id = policy.newSessionId();
        TimeUnit timeOutUnit = parameters.timeOutUnit();
        long timeOut = policy.boundTimeout(parameters.timeOut(), timeOutUnit);
        byte[] passwd = policy.newPassword(id);
        parameters = SessionParameters.create(timeOut, passwd, timeOutUnit);
        return newSession(id, parameters);
    }

    protected Session newSession(long id, SessionParameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = sessions.put(id, session);
        assert (prev == null);
        if (logger.isDebugEnabled()) {
            logger.debug("Added session: {}", session);
        }
        return session;
    }
}
