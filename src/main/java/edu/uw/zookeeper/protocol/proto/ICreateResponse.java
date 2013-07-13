package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.proto.CreateResponse;

@Operational(value=OpCode.CREATE)
public class ICreateResponse extends ICodedRecord<CreateResponse> implements Records.Response, Records.MultiOpResponse, Records.PathGetter {

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
        return get().getPath();
    }
}