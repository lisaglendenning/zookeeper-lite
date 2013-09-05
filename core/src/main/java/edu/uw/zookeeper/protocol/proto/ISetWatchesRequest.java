package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.proto.SetWatches;

@Operational(value=OpCode.SET_WATCHES)
@OperationalXid(value=OpCodeXid.SET_WATCHES)
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
        return record.getRelativeZxid();
    }

    public List<String> getDataWatches() {
        return record.getDataWatches();
    }

    public List<String> getExistWatches() {
        return record.getExistWatches();
    }

    public List<String> getChildWatches() {
        return record.getChildWatches();
    }
}