package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.CLOSE_SESSION)
@Shared
public class IDisconnectResponse extends IOperationalRecord<EmptyRecord> implements Records.Response {
    public IDisconnectResponse() {
        super(EmptyRecord.getInstance());
    }
}