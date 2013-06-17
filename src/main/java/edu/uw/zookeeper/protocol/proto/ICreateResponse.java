package edu.uw.zookeeper.protocol.proto;


import org.apache.zookeeper.proto.CreateResponse;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CREATE)
public class ICreateResponse extends IOperationalRecord<CreateResponse> implements Operation.Response, Records.MultiOpResponse, Records.PathHolder {

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