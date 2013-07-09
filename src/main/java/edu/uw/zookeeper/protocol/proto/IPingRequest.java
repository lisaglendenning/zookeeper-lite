package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.PING)
@OperationalXid(xid=OpCodeXid.PING)
@Shared
public class IPingRequest extends IOpCodeXidRecord<EmptyRecord> implements Records.Request {
    public IPingRequest() {
        super(EmptyRecord.getInstance());
    }
}