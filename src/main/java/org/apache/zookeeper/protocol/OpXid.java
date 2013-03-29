package org.apache.zookeeper.protocol;

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
