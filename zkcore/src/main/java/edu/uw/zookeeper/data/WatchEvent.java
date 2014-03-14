
package edu.uw.zookeeper.data;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;

public abstract class WatchEvent {

    public static WatchEvent fromRecord(IWatcherEvent record) {
        return of(
                EventType.fromInt(record.getType()), 
                KeeperState.fromInt(record.getState()), 
                (ZNodePath) ZNodeLabelVector.fromString(record.getPath()));
    }
    
    public static WatchEvent of(EventType eventType, KeeperState keeperState, ZNodePath path) {
        if (eventType == EventType.None) {
            return KeeperWatchEvent.of(keeperState);
        } else {
            return NodeWatchEvent.of(path, eventType);
        }
    }

    public Message.ServerResponse<IWatcherEvent> toMessage() {
        return ProtocolResponseMessage.of(
                OpCodeXid.NOTIFICATION.xid(), 
                OpCodeXid.NOTIFICATION_ZXID,
                new IWatcherEvent(
                    getEventType().getIntValue(), 
                    getKeeperState().getIntValue(), 
                    getPath().toString()));
    }

    public abstract KeeperState getKeeperState();
    
    public abstract EventType getEventType();
    
    public abstract ZNodePath getPath();
}
