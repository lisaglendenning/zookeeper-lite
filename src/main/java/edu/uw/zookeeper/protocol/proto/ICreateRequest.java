package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.proto.CreateRequest;

@Operational(value=OpCode.CREATE)
public class ICreateRequest extends IOperationalRecord<CreateRequest> implements Records.Request, Records.CreateModeGetter, Records.MultiOpRequest {

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
        return record.getPath();
    }

    @Override
    public byte[] getData() {
        return record.getData();
    }

    @Override
    public List<ACL> getAcl() {
        return record.getAcl();
    }

    @Override
    public int getFlags() {
        return record.getFlags();
    }
}
