package edu.uw.zookeeper.data;

import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.Records.OperationXid;

public abstract class OpXid implements Operation.CallRequest {
    @Override
    public Operation operation() {
        return opXid().operation();
    }

    @Override
    public int xid() {
        return opXid().xid();
    }

    public abstract Records.OperationXid opXid();
}
