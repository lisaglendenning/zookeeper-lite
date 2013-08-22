package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SyncRequest;

@Operational(value=OpCode.SYNC)
public class ISyncRequest extends ICodedRecord<SyncRequest> implements Records.Request, Records.PathGetter {
    
    public ISyncRequest() {
        this(new SyncRequest());
    }
    
    public ISyncRequest(String path) {
        this(new SyncRequest(path));
    }

    public ISyncRequest(SyncRequest record) {
        super(record);
    }

    @Override
    public String getPath() {
        return record.getPath();
    }
}