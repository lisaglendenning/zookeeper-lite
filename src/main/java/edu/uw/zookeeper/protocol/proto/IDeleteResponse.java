package edu.uw.zookeeper.protocol.proto;

@Operational(opcode=OpCode.DELETE)
@Shared
public class IDeleteResponse extends ICodedRecord<EmptyRecord> implements Records.Response, Records.MultiOpResponse {
    public IDeleteResponse() {
        super(EmptyRecord.getInstance());
    }
}