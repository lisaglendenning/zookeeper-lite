package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.SetACLRequest;

@Operational(value=OpCode.SET_ACL)
public class ISetACLRequest extends ICodedRecord<SetACLRequest> implements Records.Request, Records.PathGetter, Records.AclGetter, Records.VersionGetter {

    public ISetACLRequest() {
        this(new SetACLRequest());
    }
    
    public ISetACLRequest(String path, List<ACL> acl, int version) {
        this(new SetACLRequest(path, acl, version));
    }

    public ISetACLRequest(SetACLRequest record) {
        super(record);
    }

    @Override
    public int getVersion() {
        return get().getVersion();
    }

    @Override
    public List<ACL> getAcl() {
        return get().getAcl();
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}