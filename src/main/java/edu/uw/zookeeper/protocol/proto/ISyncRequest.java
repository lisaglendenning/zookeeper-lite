package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SyncRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.SYNC)
public class ISyncRequest extends IOperationalRecord<SyncRequest> implements Operation.Request, Records.PathHolder {
    
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
        return get().getPath();
    }
}