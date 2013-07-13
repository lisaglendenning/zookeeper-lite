package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.PING)
@OperationalXid(value=OpCodeXid.PING)
@Shared
public class IPingResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public IPingResponse() {
        super(EmptyRecord.getInstance());
    }
}