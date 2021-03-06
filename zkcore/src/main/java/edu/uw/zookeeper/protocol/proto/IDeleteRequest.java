package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.DeleteRequest;

@Operational(value=OpCode.DELETE)
public class IDeleteRequest extends IOperationalRecord<DeleteRequest> implements Records.Request, Records.MultiOpRequest, Records.PathGetter, Records.VersionGetter {

    public IDeleteRequest() {
        this(new DeleteRequest());
    }
    
    public IDeleteRequest(String path, int version) {
        this(new DeleteRequest(path, version));
    }

    public IDeleteRequest(DeleteRequest record) {
        super(record);
    }

    @Override
    public String getPath() {
        return record.getPath();
    }

    @Override
    public int getVersion() {
        return record.getVersion();
    }
}