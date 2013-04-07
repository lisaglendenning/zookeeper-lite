package org.apache.zookeeper.data;

import org.apache.zookeeper.protocol.Records;
import org.apache.zookeeper.protocol.Records.OperationXid;

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
