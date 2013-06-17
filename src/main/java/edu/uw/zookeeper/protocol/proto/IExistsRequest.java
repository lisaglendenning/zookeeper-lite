package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ExistsRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.EXISTS)
public class IExistsRequest extends IOperationalRecord<ExistsRequest> implements Operation.Request, Records.PathHolder, Records.WatchHolder {

    public IExistsRequest() {
        this(new ExistsRequest());
    }
    
    public IExistsRequest(String path, boolean watch) {
        this(new ExistsRequest(path, watch));
    }
    
    public IExistsRequest(ExistsRequest record) {
        super(record);
    }

    @Override
    public boolean getWatch() {
        return get().getWatch();
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}