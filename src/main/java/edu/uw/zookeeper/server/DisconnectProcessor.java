package edu.uw.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processors.FilteredProcessor;
import edu.uw.zookeeper.util.Processors.FilteringProcessor;


public class DisconnectProcessor extends OpRequestProcessor {

    public static FilteringProcessor<Records.Request, Records.Response> filtered(
            long sessionId, SessionManager sessions) {
        return FilteredProcessor.newInstance(
                EqualsFilter.newInstance(OpCode.CLOSE_SESSION),
                new DisconnectProcessor(sessionId, sessions));
    }
    
    public static DisconnectProcessor newInstance(
            long sessionId, SessionManager sessions) {
        return new DisconnectProcessor(sessionId, sessions);
    }

    protected final Logger logger = LoggerFactory
            .getLogger(DisconnectProcessor.class);
    protected final long sessionId;
    protected final SessionManager sessions;

    protected DisconnectProcessor(long sessionId, SessionManager sessions) {
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
    public Records.Response apply(Records.Request request) {
        if (request.opcode() != OpCode.CLOSE_SESSION) {
            return null;
        }
        
        if (sessions().remove(sessionId()) == null) {
            throw new IllegalStateException(String.format("Session %l not found", sessionId()));
        }
        return super.apply(request);
    }
}
