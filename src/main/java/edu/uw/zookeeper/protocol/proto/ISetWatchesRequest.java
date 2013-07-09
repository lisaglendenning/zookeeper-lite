package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.proto.SetWatches;

@Operational(opcode=OpCode.SET_WATCHES)
@OperationalXid(xid=OpCodeXid.SET_WATCHES)
public class ISetWatchesRequest extends IOpCodeXidRecord<SetWatches> implements Records.Request {

    public ISetWatchesRequest() {
        this(new SetWatches());
    }

    public ISetWatchesRequest(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches) {
        this(new SetWatches(relativeZxid, dataWatches, existWatches, childWatches));
    }

    public ISetWatchesRequest(SetWatches record) {
        super(record);
    }

    public long getRelativeZxid() {
        return get().getRelativeZxid();
    }

    public List<String> getDataWatches() {
        return get().getDataWatches();
    }

    public List<String> getExistWatches() {
        return get().getExistWatches();
    }

    public List<String> getChildWatches() {
        return get().getChildWatches();
    }
}