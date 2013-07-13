package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.CheckVersionRequest;

@Operational(value=OpCode.CHECK)
public class ICheckVersionRequest extends ICodedRecord<CheckVersionRequest> implements Records.Request, Records.MultiOpRequest, Records.PathGetter, Records.VersionGetter {

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