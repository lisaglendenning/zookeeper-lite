package edu.uw.zookeeper.data;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

public interface TxnOperation extends Operation.ResponseId {

    long getTime();
    
    public interface Request<T extends Records.Request> extends TxnOperation, SessionOperation.Request<T> {
    }

    public interface Response<T extends Records.Response> extends TxnOperation, SessionOperation.Response<T> {
    }
}
