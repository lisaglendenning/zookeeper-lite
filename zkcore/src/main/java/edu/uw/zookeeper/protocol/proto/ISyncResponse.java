package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SyncResponse;

@Operational(value=OpCode.SYNC)
public class ISyncResponse extends IOperationalRecord<SyncResponse> implements Records.Response, Records.PathGetter {
    
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
        return record.getPath();
    }
}