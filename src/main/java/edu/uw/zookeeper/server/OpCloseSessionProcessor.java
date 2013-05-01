package edu.uw.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.protocol.OpCode;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.util.Processors.FilteredProcessor;
import edu.uw.zookeeper.util.Processors.FilteringProcessor;

public class OpCloseSessionProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Operation.Request, Operation.Response> filtered(
            long sessionId, SessionManager sessions) {
        return FilteredProcessor.newInstance(
                EqualsFilter.newInstance(OpCode.CLOSE_SESSION),
                new OpCloseSessionProcessor(sessionId, sessions));
    }
    
    public static OpCloseSessionProcessor newInstance(
            long sessionId, SessionManager sessions) {
        return new OpCloseSessionProcessor(sessionId, sessions);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(OpCloseSessionProcessor.class);
    protected final long sessionId;
    protected final SessionManager sessions;

    protected OpCloseSessionProcessor(long sessionId, SessionManager sessions) {
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
    public Operation.Response apply(Operation.Request request) {
        if (request.opcode() != OpCode.CLOSE_SESSION) {
            return null;
        }
        
        if (sessions().remove(sessionId()) == null) {
            throw new IllegalStateException(String.format("Session %l not found", sessionId()));
        }
        return super.apply(request);
    }
}
