package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetChildren2Request;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.GET_CHILDREN2)
public class IGetChildren2Request extends IOperationalRecord<GetChildren2Request> implements Operation.Request, Records.PathHolder, Records.WatchHolder {
    
    public IGetChildren2Request() {
        this(new GetChildren2Request());
    }
    
    public IGetChildren2Request(String path, boolean watch) {
        this(new GetChildren2Request(path, watch));
    }

    public IGetChildren2Request(GetChildren2Request record) {
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