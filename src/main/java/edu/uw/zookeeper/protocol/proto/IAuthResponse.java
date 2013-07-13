package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.AUTH)
@OperationalXid(value=OpCodeXid.AUTH)
@Shared
public class IAuthResponse extends IOpCodeXidRecord<EmptyRecord> implements Records.Response {
    public IAuthResponse() {
        super(EmptyRecord.getInstance());
    }
}