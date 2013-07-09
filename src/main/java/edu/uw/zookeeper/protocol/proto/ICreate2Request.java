package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.Create2Request;

@Operational(opcode=OpCode.CREATE2)
public class ICreate2Request extends ICodedRecord<Create2Request> implements Records.Request, Records.CreateModeGetter, Records.MultiOpRequest {

    public ICreate2Request() {
        this(new Create2Request());
    }
    
    public ICreate2Request(String path, byte[] data, List<ACL> acl, int flags) {
        this(new Create2Request(path, data, acl, flags));
    }
    
    public ICreate2Request(Create2Request record) {
        super(record);
    }

    @Override
    public String getPath() {
        return get().getPath();
    }

    @Override
    public byte[] getData() {
        return get().getData();
    }

    @Override
    public List<ACL> getAcl() {
        return get().getAcl();
    }

    @Override
    public int getFlags() {
        return get().getFlags();
    }
}
