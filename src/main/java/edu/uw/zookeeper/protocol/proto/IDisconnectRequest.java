package edu.uw.zookeeper.protocol.proto;


import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CLOSE_SESSION)
@Shared
public class IDisconnectRequest extends IOperationalRecord<EmptyRecord> implements Operation.Request {
    public IDisconnectRequest() {
        super(EmptyRecord.getInstance());
    }
}