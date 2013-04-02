package org.apache.zookeeper.server;

import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.Operation;

public class SessionRequestExecutor extends RequestExecutor {

    public static SessionRequestExecutor create(ExecutorService executor,
            Zxid zxid, SessionManager sessions,
            long sessionId) {
        return new SessionRequestExecutor(executor, zxid, sessions, sessionId);
    }

    protected final long sessionId;
    
    protected SessionRequestExecutor(ExecutorService executor, 
            Zxid zxid, SessionManager sessions,
            long sessionId) {
        super(executor, zxid, sessions);
        this.sessionId = sessionId;
    }

    public long sessionId() {
        return sessionId;
    }
    
    public OpResultTask createTask(Operation.Request request) {
        switch (request.operation()) {
        case CLOSE_SESSION:
            return createTask(OpCloseSessionTask.create(request, sessionId(), sessions));
        default:
            return super.createTask(request);
        }
    }
}
