package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.AUTH)
@Shared
public class IAuthResponse extends IOperationalXidRecord<EmptyRecord> implements Operation.Response, Operation.XidHeader {
    public IAuthResponse() {
        super(EmptyRecord.getInstance());
    }
}