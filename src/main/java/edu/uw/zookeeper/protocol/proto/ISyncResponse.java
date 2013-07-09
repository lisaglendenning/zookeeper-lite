package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SyncResponse;

@Operational(opcode=OpCode.SYNC)
public class ISyncResponse extends ICodedRecord<SyncResponse> implements Records.Response, Records.PathGetter {
    
    public ISyncResponse() {
        this(new SyncResponse());
    }
    
    public ISyncResponse(String path) {
        this(new SyncResponse(path));
    }

    public ISyncResponse(SyncResponse record) {
        super(record);
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}