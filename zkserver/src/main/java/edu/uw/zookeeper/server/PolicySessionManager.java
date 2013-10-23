package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.*;

import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public abstract class PolicySessionManager extends AbstractSessionManager {

    protected PolicySessionManager() {}
    
    @Override
    public Session newSession() {
        long id = policy().newSessionId();
        TimeValue timeOut = policy().maxTimeout();
        byte[] passwd = policy().newPassword(id);
        Session.Parameters parameters = Session.Parameters.create(timeOut,
                passwd);
        return newSession(id, parameters);
    }

    @Override
    public Session validate(Session session) {
        checkNotNull(session);
        if (Session.uninitialized().id() == session.id()) {
            return validate(session.parameters());
        } else {
            // TODO: maybe disallow session renewal if it is expired or not present?
            checkArgument(policy().validatePassword(session.id(), session.parameters().password()));
            put(session);
            return session;
        }
    }

    @Override
    public Session validate(Session.Parameters parameters) {
        long id = policy().newSessionId();
        TimeValue timeOut = policy().boundTimeout(parameters.timeOut());
        byte[] passwd = policy().newPassword(id);
        parameters = Session.Parameters.create(timeOut, passwd);
        return newSession(id, parameters);
    }

    protected Session newSession(long id, Session.Parameters parameters) {
        Session session = Session.create(id, parameters);
        Session prev = put(session);
        assert (prev == null);
        return session;
    }
    
    protected abstract Session put(Session session);

    protected abstract SessionParametersPolicy policy();
}
