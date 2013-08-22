package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetChildrenRequest;

@Operational(value=OpCode.GET_CHILDREN)
public class IGetChildrenRequest extends IOperationalRecord<GetChildrenRequest> implements Records.Request, Records.PathGetter, Records.WatchGetter {

    public IGetChildrenRequest() {
        this(new GetChildrenRequest());
    }

    public IGetChildrenRequest(String path, boolean watch) {
        this(new GetChildrenRequest(path, watch));
    }

    public IGetChildrenRequest(GetChildrenRequest record) {
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