package edu.uw.zookeeper.protocol.proto;

import java.util.List;

import org.apache.zookeeper.proto.SetWatches;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.SET_WATCHES)
public class ISetWatchesRequest extends IOperationalXidRecord<SetWatches> implements Operation.Request, Operation.XidHeader {

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
}