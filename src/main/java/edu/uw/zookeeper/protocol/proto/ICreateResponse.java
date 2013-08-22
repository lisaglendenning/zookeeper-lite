package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.proto.CreateResponse;

@Operational(value=OpCode.CREATE)
public class ICreateResponse extends IOperationalRecord<CreateResponse> implements Records.Response, Records.MultiOpResponse, Records.PathGetter {

    public ICreateResponse() {
        this(new CreateResponse());
    }
    
    public ICreateResponse(String path) {
        this(new CreateResponse(path));
    }

    public ICreateResponse(CreateResponse record) {
        super(record);
    }

    @Override
    public String getPath() {
        return record.getPath();
    }
}