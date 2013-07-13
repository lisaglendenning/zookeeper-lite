package edu.uw.zookeeper.data;

import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public enum WatchEventProcessor implements Processor<Records.Request, WatchEvent[]> {
    INSTANCE;
    
    public static WatchEventProcessor getInstance() {
        return INSTANCE;
    }
    
    @Override
    public WatchEvent[] apply(Records.Request input) {
        WatchEvent[] result = new WatchEvent[0];
        switch (input.getOpcode()) {
        case CREATE:
        case CREATE2:
        {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            result = (path.isRoot()) ? new WatchEvent[1] : new WatchEvent[2];
            result[0] = WatchEvent.created(path);
            if (! path.isRoot()) {
                result[1] = WatchEvent.children((ZNodeLabel.Path) path.head());
            }
            break;
        }
        case DELETE:
        {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            result = (path.isRoot()) ? new WatchEvent[1] : new WatchEvent[2];
            result[0] = WatchEvent.deleted(path);
            if (! path.isRoot()) {
                result[1] = WatchEvent.children((ZNodeLabel.Path) path.head());
            }
            break;
        }
        case SET_DATA:
        {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) input).getPath());
            result = new WatchEvent[1];
            result[0] = WatchEvent.data(path);
            break;
        }
        default:
            break;
        }
        return result;
    }
}
