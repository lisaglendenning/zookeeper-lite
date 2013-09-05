package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.protocol.proto.Records;

public interface SessionOperation {

    long getSessionId();
    
    public interface Request<T extends Records.Request> extends SessionOperation, Operation.ProtocolRequest<T> {
    }

    public interface Response<T extends Records.Response> extends SessionOperation, Operation.ProtocolResponse<T> {
    }
}
