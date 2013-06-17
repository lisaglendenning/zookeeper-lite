package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CHECK)
@Shared
public class ICheckVersionResponse extends IOperationalRecord<EmptyRecord> implements Operation.Response, Records.MultiOpResponse {
    public ICheckVersionResponse() {
        super(EmptyRecord.getInstance());
    }
}