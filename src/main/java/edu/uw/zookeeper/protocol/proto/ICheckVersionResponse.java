package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.CHECK)
@Shared
public class ICheckVersionResponse extends ICodedRecord<EmptyRecord> implements Records.Response, Records.MultiOpResponse {
    public ICheckVersionResponse() {
        super(EmptyRecord.getInstance());
    }
}