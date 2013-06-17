package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.SyncResponse;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.SYNC)
public class ISyncResponse extends IOperationalRecord<SyncResponse> implements Operation.Response, Records.PathHolder {
    
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