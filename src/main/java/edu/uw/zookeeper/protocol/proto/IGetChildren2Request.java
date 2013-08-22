package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetChildren2Request;

@Operational(value=OpCode.GET_CHILDREN2)
public class IGetChildren2Request extends ICodedRecord<GetChildren2Request> implements Records.Request, Records.PathGetter, Records.WatchGetter {
    
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
        return record.getWatch();
    }

    @Override
    public String getPath() {
        return record.getPath();
    }
}