package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.event.SessionStateEvent;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.TimeValue;

public class ConcurrentSessionTable extends ForwardingEventful implements SessionTable {

    public static ConcurrentSessionTable newInstance(Publisher publisher,
            SessionParametersPolicy policy) {
        return new ConcurrentSessionTable(publisher, policy);
    }

    protected final Logger logger;
    protected final SessionParametersPolicy policy;
    protected final ConcurrentMap<Long, Session> sessions;

    protected ConcurrentSessionTable(Publisher publisher, SessionParametersPolicy policy) {
        this(publisher, policy, Maps.<Long, Session>newConcurrentMap());
    }

    protected ConcurrentSessionTable(Publisher publisher, SessionParametersPolicy policy,
            ConcurrentMap<Long, Session> sessions) {
        super(publisher);
        this.logger = LoggerFactory.getLogger(getClass());
        this.policy = policy;
        this.sessions = sessions;
    }

    @Override
    public Iterator<Session> iterator() {
        return sessions.values().iterator();
    }

    @Override
    public Session newSession() {
        long id = policy.newSessionId();
        TimeValue timeOut = policy.maxTimeout();
        byte[] passwd = policy.newPassword(id);
        Session.Parameters parameters = Session.Parameters.create(timeOut,
                passwd);
        return newSession(id, parameters);
    }

    @Override
    public Session validate(Session session) {
        checkNotNull(session);
        if (session.initialized()) {
            // TODO: maybe disallow session renewal if it is expired or not in the table?
            checkArgument(policy.validatePassword(session.id(), session.parameters().password()));
            Session prev = sessions.put(session.id(), session);
            if (prev != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Updating Session {} to {}", session, prev);
                }
            }
            return session;
        } else {
            return validate(session.parameters());
        }
    }

    @Override
    public Session validate(Session.Parameters parameters) {
        long id = policy.newSessionId();
        TimeValue timeOut = policy.boundTimeout(parameters.timeOut());
        byte[] passwd = policy.newPassword(id);
        parameters = Session.Parameters.create(timeOut, passwd);
        return newSession(id, parameters);
    }

    @Override
    public Session get(long id) {
        return sessions.get(id);
    }

    @Override
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

    protected Session newSession(long id, Session.Parameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = sessions.put(id, session);
        assert (prev == null);
        if (logger.isDebugEnabled()) {
            logger.debug("Created session: {}", session);
        }
        post(SessionStateEvent.create(session, Session.State.SESSION_OPENED));
        return session;
    }
}
