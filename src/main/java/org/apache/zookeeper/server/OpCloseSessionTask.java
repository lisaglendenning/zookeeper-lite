package org.apache.zookeeper.server;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Session;
import org.apache.zookeeper.protocol.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;


public class OpCloseSessionTask extends OpRequestTask {

    public static OpCloseSessionTask create(Operation.Request request, long sessionId, SessionManager sessions) {
        return new OpCloseSessionTask(request, sessionId, sessions);
    }
    
    protected final Logger logger = LoggerFactory.getLogger(OpCloseSessionTask.class);
    protected final long sessionId;
    protected final SessionManager sessions;
    
    @Inject
    protected OpCloseSessionTask(Operation.Request request, long sessionId, SessionManager sessions) {
        super(request);
        this.sessionId = sessionId;
        this.sessions = sessions;
    }

    public long sessionId() {
        return sessionId;
    }
    
    public SessionManager sessions() {
        return sessions;
    }

    @Override
    public Operation.Response call() throws Exception {
        if (sessions().remove(sessionId()) == null) {
            throw new KeeperException.SessionExpiredException();
        }
        return super.call();
    }
}
