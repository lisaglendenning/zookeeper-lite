package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.CLOSE_SESSION)
@Shared
public class IDisconnectResponse extends ICodedRecord<EmptyRecord> implements Records.Response {
    public IDisconnectResponse() {
        super(EmptyRecord.getInstance());
    }
}