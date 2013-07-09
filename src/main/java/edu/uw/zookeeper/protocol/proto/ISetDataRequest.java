package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SetDataRequest;

@Operational(opcode=OpCode.SET_DATA)
public class ISetDataRequest extends ICodedRecord<SetDataRequest> implements Records.Request, Records.PathGetter, Records.DataGetter, Records.VersionGetter, Records.MultiOpRequest {
    
    public ISetDataRequest() {
        this(new SetDataRequest());
    }
    
    public ISetDataRequest(String path, byte[] data, int version) {
        this(new SetDataRequest(path, data, version));
    }

    public ISetDataRequest(SetDataRequest record) {
        super(record);
    }

    @Override
    public int getVersion() {
        return get().getVersion();
    }

    @Override
    public byte[] getData() {
        return get().getData();
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}
