package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.RemoveWatchesRequest;

@Operational(value=OpCode.REMOVE_WATCHES)
public class IRemoveWatchesRequest extends IOperationalRecord<RemoveWatchesRequest> implements Records.Request, Records.PathGetter {

    public IRemoveWatchesRequest() {
        this(new RemoveWatchesRequest());
    }
    
    public IRemoveWatchesRequest(String path, int type) {
        this(new RemoveWatchesRequest(path, type));
    }
    
    public IRemoveWatchesRequest(RemoveWatchesRequest record) {
        super(record);
    }

    @Override
    public String getPath() {
        return record.getPath();
    }
    
    public int getType() {
        return record.getType();
    }
}
