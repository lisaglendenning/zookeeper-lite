package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.SET_WATCHES)
@OperationalXid(xid=OpCodeXid.SET_WATCHES)
@Shared
public class ISetWatchesResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public ISetWatchesResponse() {
        super(EmptyRecord.getInstance());
    }
}