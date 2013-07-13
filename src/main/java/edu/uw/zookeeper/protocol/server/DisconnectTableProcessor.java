package edu.uw.zookeeper.protocol.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.proto.IDisconnectRequest;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SessionTable;


public class DisconnectTableProcessor implements TxnRequestProcessor<IDisconnectRequest, Records.Response> {

    public static DisconnectTableProcessor newInstance(
            SessionTable sessions) {
        return new DisconnectTableProcessor(sessions);
    }

    protected final Logger logger;
    protected final SessionTable sessions;

    protected DisconnectTableProcessor(SessionTable sessions) {
        this.logger = LoggerFactory.getLogger(getClass());
        this.sessions = sessions;
    }

    public SessionTable sessions() {
        return sessions;
    }

    @Override
    public Records.Response apply(TxnOperation.Request<IDisconnectRequest> request) {
        if (sessions().remove(request.getSessionId()) == null) {
            throw new IllegalStateException(String.format("Session %l not found", request.getSessionId()));
        }
        return Records.Responses.getInstance().get(OpCode.CLOSE_SESSION);
    }
}
