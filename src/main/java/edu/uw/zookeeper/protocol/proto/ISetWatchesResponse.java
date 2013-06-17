package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.SET_WATCHES)
@Shared
public class ISetWatchesResponse extends IOperationalXidRecord<EmptyRecord> implements Operation.Response, Operation.XidHeader {
    public ISetWatchesResponse() {
        super(EmptyRecord.getInstance());
    }
}