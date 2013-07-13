package edu.uw.zookeeper.protocol.proto;

import org.apache.zookeeper.proto.WatcherEvent;

@Operational(value=OpCode.NOTIFICATION)
@OperationalXid(value=OpCodeXid.NOTIFICATION)
public class IWatcherEvent extends IOpCodeXidRecord<WatcherEvent> implements Records.Response, Records.PathGetter {

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