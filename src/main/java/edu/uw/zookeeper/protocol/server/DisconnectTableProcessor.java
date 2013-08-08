package edu.uw.zookeeper.protocol.server;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.protocol.proto.IDisconnectResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.server.SessionTable;


public class DisconnectTableProcessor implements Processors.CheckedProcessor<Long, IDisconnectResponse, KeeperException> {

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
    public IDisconnectResponse apply(Long sessionId) throws KeeperException {
        if (sessions().remove(sessionId) == null) {
            throw new KeeperException.SessionMovedException();
        }
        return Records.newInstance(IDisconnectResponse.class);
    }
}
