package edu.uw.zookeeper.server;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import edu.uw.zookeeper.Session;
import edu.uw.zookeeper.SessionRequestExecutor;
import edu.uw.zookeeper.protocol.OpAction;
import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionRequestWrapper;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.Publisher;

public class ServerSessionRequestExecutor extends ForwardingEventful implements SessionRequestExecutor, Publisher {

    public static ServerSessionRequestExecutor newInstance(
            Publisher publisher, ExpiringSessionManager sessions, long sessionId) {
        return new ServerSessionRequestExecutor(publisher, sessions, sessionId);
    }
    
    protected final long sessionId;
    protected final ExpiringSessionManager sessions;
    
    protected ServerSessionRequestExecutor(
            Publisher publisher,
            ExpiringSessionManager sessions,
            long sessionId) {
        super(publisher);
        this.sessionId = sessionId;
        this.sessions = sessions;
    }
    
    public ExpiringSessionManager sessions() {
        return sessions;
    }

    @Override
    public void post(Object event) {
        if (event == Session.State.SESSION_EXPIRED) {
            try {
                submit(SessionRequestWrapper.create(0, OpAction.Request.create(OpCode.CLOSE_SESSION)));
            } catch (Exception e) {
                // TODO
                Throwables.propagate(e);
            }
        }
        super.post(event);
    }
    
    @Override
    public ListenableFuture<Operation.SessionReply> submit(Operation.SessionRequest request) {
        sessions().touch(sessionId);
        System.out.printf("0x%s: %s%n", Long.toHexString(sessionId), request);
        return SettableFuture.create();
    }
}
