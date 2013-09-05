package edu.uw.zookeeper.protocol.proto;

@Operational(value=OpCode.DELETE)
@Shared
public class IDeleteResponse extends IOperationalRecord<EmptyRecord> implements Records.Response, Records.MultiOpResponse {
    public IDeleteResponse() {
        super(EmptyRecord.getInstance());
    }
}