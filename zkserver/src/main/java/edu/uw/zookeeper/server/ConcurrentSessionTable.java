package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.ConcurrentMap;

import net.engio.mbassy.PubSubSupport;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.collect.Maps;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public class ConcurrentSessionTable extends SessionTableAdapter implements SessionTable {

    public static ConcurrentSessionTable newInstance(PubSubSupport<Object> publisher,
            SessionParametersPolicy policy) {
        return new ConcurrentSessionTable(publisher, policy);
    }

    protected final Logger logger;
    protected final PubSubSupport<Object> publisher;
    protected final SessionParametersPolicy policy;
    protected final ConcurrentMap<Long, Session> sessions;

    protected ConcurrentSessionTable(PubSubSupport<Object> publisher, SessionParametersPolicy policy) {
        this(publisher, policy, Maps.<Long, Session>newConcurrentMap());
    }

    protected ConcurrentSessionTable(PubSubSupport<Object> publisher, SessionParametersPolicy policy,
            ConcurrentMap<Long, Session> sessions) {
        this.logger = LogManager.getLogger(getClass());
        this.publisher = publisher;
        this.policy = policy;
        this.sessions = sessions;
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
            put(session);
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
    public Session remove(long id) {
        Session session = super.remove(id);
        if (logger.isDebugEnabled()) {
            if (session != null) {
                logger.debug("Removed session: {}", session);
            }
        }
        return session;
    }

    protected Session newSession(long id, Session.Parameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = put(session);
        assert (prev == null);
        return session;
    }
    
    @Override
    protected Session put(Session session) {
        Session prev = super.put(session);
        if (logger.isDebugEnabled()) {
            if (prev == null) {
                logger.debug("Created session: {}", session);
            } else {
                logger.debug("Updating session: {} to {}", session, prev);
            }
        }
        return prev;
    }

    @Override
    protected ConcurrentMap<Long, Session> sessions() {
        return sessions;
    }

    @Override
    protected PubSubSupport<Object> publisher() {
        return publisher;
    }
}
