package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.AUTH)
@OperationalXid(xid=OpCodeXid.AUTH)
@Shared
public class IAuthResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public IAuthResponse() {
        super(EmptyRecord.getInstance());
    }
}