package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetACLRequest;

@Operational(value=OpCode.GET_ACL)
public class IGetACLRequest extends ICodedRecord<GetACLRequest> implements Records.Request, Records.PathGetter {
    
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
        return record.getPath();
    }
}