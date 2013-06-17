package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.PING)
@Shared
public class IPingRequest extends IOperationalXidRecord<EmptyRecord> implements Operation.Request, Operation.XidHeader {
    public IPingRequest() {
        super(EmptyRecord.getInstance());
    }
}