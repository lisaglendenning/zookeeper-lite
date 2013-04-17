package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.inject.Inject;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.TimeValue;

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
        TimeValue timeOut = policy.maxTimeout();
        byte[] passwd = policy.newPassword(id);
        Session.Parameters parameters = Session.Parameters.create(timeOut,
                passwd);
        Session session = newSession(id, parameters);
        post(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
        return session;
    }

    public Session add(long id, Session.Parameters parameters) {
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

    public Session add(Session.Parameters parameters) {
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

    protected Session newSession(Session.Parameters parameters) {
        long id = policy.newSessionId();
        TimeValue timeOut = policy.boundTimeout(parameters.timeOut());
        byte[] passwd = policy.newPassword(id);
        parameters = Session.Parameters.create(timeOut, passwd);
        return newSession(id, parameters);
    }

    protected Session newSession(long id, Session.Parameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = sessions.put(id, session);
        assert (prev == null);
        if (logger.isDebugEnabled()) {
            logger.debug("Added session: {}", session);
        }
        return session;
    }
}
