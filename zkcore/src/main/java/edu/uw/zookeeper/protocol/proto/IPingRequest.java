package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.PING)
@OperationalXid(value=OpCodeXid.PING)
@Shared
public class IPingRequest extends IOpCodeXidRecord<EmptyRecord> implements Records.Request {
    public IPingRequest() {
        super(EmptyRecord.getInstance());
    }
}