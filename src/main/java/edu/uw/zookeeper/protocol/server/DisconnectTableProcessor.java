package edu.uw.zookeeper.protocol.server;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import edu.uw.zookeeper.common.Processor;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.IDisconnectRequest;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SessionTable;


public class DisconnectTableProcessor implements Processor<SessionOperation.Request<IDisconnectRequest>, IDisconnectResponse> {

    public static DisconnectTableProcessor newInstance(
            SessionTable sessions) {
        return new DisconnectTableProcessor(sessions);
    }

    protected final Logger logger;
    protected final SessionTable sessions;

    protected DisconnectTableProcessor(SessionTable sessions) {
        this.logger = LogManager.getLogger(getClass());
        this.sessions = sessions;
    }

    public SessionTable sessions() {
        return sessions;
    }

    @Override
    public IDisconnectResponse apply(SessionOperation.Request<IDisconnectRequest> request) {
        if (sessions().remove(request.getSessionId()) == null) {
            throw new IllegalStateException(String.format("Session %l not found", request.getSessionId()));
        }
        return Records.newInstance(IDisconnectResponse.class);
    }
}
