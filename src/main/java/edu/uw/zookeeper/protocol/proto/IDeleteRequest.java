package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.DeleteRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.DELETE)
public class IDeleteRequest extends IOperationalRecord<DeleteRequest> implements Operation.Request, Records.MultiOpRequest, Records.PathHolder, Records.VersionHolder {

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
        return get().getPath();
    }

    @Override
    public int getVersion() {
        return get().getVersion();
    }
}