package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.ExistsRequest;

@Operational(opcode=OpCode.EXISTS)
public class IExistsRequest extends ICodedRecord<ExistsRequest> implements Records.Request, Records.PathGetter, Records.WatchGetter {

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