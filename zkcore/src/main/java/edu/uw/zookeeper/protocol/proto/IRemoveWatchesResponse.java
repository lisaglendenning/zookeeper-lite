package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.REMOVE_WATCHES)
@Shared
public class IRemoveWatchesResponse extends IOperationalRecord<EmptyRecord> implements Records.Response {
    public IRemoveWatchesResponse() {
        super(EmptyRecord.getInstance());
    }
}