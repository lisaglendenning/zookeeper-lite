package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.GetChildrenRequest;

import edu.uw.zookeeper.protocol.Operation;

@Operational(opcode=OpCode.GET_CHILDREN)
public class IGetChildrenRequest extends IOperationalRecord<GetChildrenRequest> implements Operation.Request, Records.PathHolder, Records.WatchHolder {

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
        return get().getWatch();
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
}