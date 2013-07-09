package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.PING)
@OperationalXid(xid=OpCodeXid.PING)
@Shared
public class IPingResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public IPingResponse() {
        super(EmptyRecord.getInstance());
    }
}