package edu.uw.zookeeper.client;

import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.client.XidIncrementer;
import edu.uw.zookeeper.protocol.proto.Records;

public class SessionClientProcessor implements Processors.UncheckedProcessor<Records.Request, SessionOperation.Request<Records.Request>>, Generator<Integer> {

    public static SessionClientProcessor create(long sessionId) {
        return new SessionClientProcessor(sessionId, XidIncrementer.fromZero());
    }
    
    protected final long sessionId;
    protected final Generator<Integer> xids;
    
    public SessionClientProcessor(long sessionId, Generator<Integer> xids) {
        super();
        this.sessionId = sessionId;
        this.xids = xids;
    }

    public long getSessionId() {
        return sessionId;
    }

    @Override
    public SessionOperation.Request<Records.Request> apply(Records.Request input) {
        int xid;
        if (input instanceof Operation.RequestId) {
            xid = ((Operation.RequestId) input).getXid();
        } else {
            xid = next();
        }
        Operation.ProtocolRequest<Records.Request> protocolRequest = ProtocolRequestMessage.of(xid, input); 
        return SessionRequest.of(getSessionId(), protocolRequest, protocolRequest);
    }

    @Override
    public Integer get() {
        return xids.get();
    }

    @Override
    public Integer next() {
        return xids.next();
    }
}
