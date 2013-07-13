package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.SET_WATCHES)
@OperationalXid(value=OpCodeXid.SET_WATCHES)
@Shared
public class ISetWatchesResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public ISetWatchesResponse() {
        super(EmptyRecord.getInstance());
    }
}