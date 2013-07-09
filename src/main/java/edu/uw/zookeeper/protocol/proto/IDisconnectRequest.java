package edu.uw.zookeeper.protocol.proto;


@Operational(opcode=OpCode.CLOSE_SESSION)
@Shared
public class IDisconnectRequest extends ICodedRecord<EmptyRecord> implements Records.Request {
    public IDisconnectRequest() {
        super(EmptyRecord.getInstance());
    }
}