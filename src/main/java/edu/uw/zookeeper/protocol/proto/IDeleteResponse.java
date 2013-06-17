package edu.uw.zookeeper.protocol.proto;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.DELETE)
@Shared
public class IDeleteResponse extends IOperationalRecord<EmptyRecord> implements Operation.Response, Records.MultiOpResponse {
    public IDeleteResponse() {
        super(EmptyRecord.getInstance());
    }
}