package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.CheckVersionRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.CHECK)
public class ICheckVersionRequest extends IOperationalRecord<CheckVersionRequest> implements Operation.Request, Records.MultiOpRequest, Records.PathHolder, Records.VersionHolder {

    public ICheckVersionRequest() {
        this(new CheckVersionRequest());
    }
    
    public ICheckVersionRequest(String path, int version) {
        this(new CheckVersionRequest(path, version));
    }
    
    public ICheckVersionRequest(CheckVersionRequest record) {
        super(record);
    }

    @Override
    public int getVersion() {
        return get().getVersion();
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}