package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CLOSE_SESSION)
@Shared
public class IDisconnectResponse extends IOperationalRecord<EmptyRecord> implements Operation.Response {
    public IDisconnectResponse() {
        super(EmptyRecord.getInstance());
    }
}