package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.PING)
@Shared
public class IPingResponse extends IOperationalXidRecord<EmptyRecord> implements Operation.Response, Operation.XidHeader {
    public IPingResponse() {
        super(EmptyRecord.getInstance());
    }
}