package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetACLRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.GET_ACL)
public class IGetACLRequest extends IOperationalRecord<GetACLRequest> implements Operation.Request, Records.PathHolder {
    
    public IGetACLRequest() {
        this(new GetACLRequest());
    }
    
    public IGetACLRequest(String path) {
        this(new GetACLRequest(path));
    }

    public IGetACLRequest(GetACLRequest record) {
        super(record);
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}