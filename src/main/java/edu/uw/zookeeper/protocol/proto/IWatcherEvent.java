package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.WatcherEvent;

import edu.uw.zookeeper.protocol.Operation;

@OperationalXid(xid=OpCodeXid.NOTIFICATION)
public class IWatcherEvent extends IOperationalXidRecord<WatcherEvent> implements Operation.Response, Records.PathHolder, Operation.XidHeader {

    public IWatcherEvent() {
        this(new WatcherEvent());
    }

    public IWatcherEvent(int type, int state, String path) {
        this(new WatcherEvent(type, state, path));
    }

    public IWatcherEvent(WatcherEvent record) {
        super(record);
    }

    @Override
    public String getPath() {
        return get().getPath();
    }
    
    public int getType() {
        return get().getType();
    }
    
    public int getState() {
        return get().getState();
    }
}