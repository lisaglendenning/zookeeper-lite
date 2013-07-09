package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetDataRequest;

@Operational(opcode=OpCode.GET_DATA)
public class IGetDataRequest extends ICodedRecord<GetDataRequest> implements Records.Request, Records.PathGetter, Records.WatchGetter {

    public IGetDataRequest() {
        this(new GetDataRequest());
    }

    public IGetDataRequest(String path, boolean watch) {
        this(new GetDataRequest(path, watch));
    }

    public IGetDataRequest(GetDataRequest record) {
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