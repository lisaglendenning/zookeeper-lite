package edu.uw.zookeeper.server;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public abstract class AbstractSessionManager implements SessionManager {

    protected final SessionParametersPolicy policy;
    
    protected AbstractSessionManager(SessionParametersPolicy policy) {
        this.policy = policy;
    }
    
    public SessionParametersPolicy policy() {
        return policy;
    }

    @Override
    public Session create() {
        long id = policy().newSessionId();
        TimeValue timeOut = policy().maxTimeout();
        byte[] passwd = policy().newPassword(id);
        Session.Parameters parameters = Session.Parameters.create(timeOut,
                passwd);
        return newSession(id, parameters);
    }

    @Override
    public Session create(TimeValue timeOut) {
        timeOut = policy().boundTimeout(timeOut);
        long id = policy().newSessionId();
        byte[] passwd = policy().newPassword(id);
        return newSession(id, Session.Parameters.create(timeOut, passwd));
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).addValue(Iterators.toString(iterator())).toString();
    }

    protected Session newSession(long id, Session.Parameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = put(session);
        assert (prev == null);
        return session;
    }
}
