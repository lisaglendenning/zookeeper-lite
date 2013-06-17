package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CreateRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CREATE)
public class ICreateRequest extends IOperationalRecord<CreateRequest> implements Operation.Request, Records.CreateHolder, Records.MultiOpRequest {

    public ICreateRequest() {
        this(new CreateRequest());
    }
    
    public ICreateRequest(String path, byte[] data, List<ACL> acl, int flags) {
        this(new CreateRequest(path, data, acl, flags));
    }

    public ICreateRequest(CreateRequest record) {
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
