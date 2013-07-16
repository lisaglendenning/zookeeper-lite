
package edu.uw.zookeeper.data;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Event;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCodeXid;
import edu.uw.zookeeper.protocol.proto.Records;

@Event
public class WatchEvent {

    public static WatchEvent of(Operation.ProtocolResponse<IWatcherEvent> message) {
        return new WatchEvent(
                message.getZxid(),
                EventType.fromInt(message.getRecord().getType()), 
                KeeperState.fromInt(message.getRecord().getState()),
                ZNodeLabel.Path.of(message.getRecord().getPath()));
    }

    public static WatchEvent of(long zxid, EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        return new WatchEvent(zxid, eventType, keeperState, path);
    }
    
    private final long zxid;
    private final ZNodeLabel.Path path;
    private final KeeperState keeperState;
    private final EventType eventType;
    
    public WatchEvent(long zxid, EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
        this.zxid = zxid;
    }
    
    public long getZxid() {
        return zxid;
    }
    
    public KeeperState getState() {
        return keeperState;
    }
    
    public EventType getType() {
        return eventType;
    }
    
    public ZNodeLabel.Path getPath() {
        return path;
    }

    public Message.ServerResponse<IWatcherEvent> toMessage() {
        return ProtocolResponseMessage.of(
                OpCodeXid.NOTIFICATION.getXid(), 
                getZxid(),
                new IWatcherEvent(
                    getType().getIntValue(), 
                    getState().getIntValue(), 
                    getPath().toString()));
    }

    @Override
    public String toString() {
        return Records.toBeanString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof WatchEvent)) {
            return false;
        }
        WatchEvent other = (WatchEvent) obj;
        return Objects.equal(getType(), other.getType())
                && Objects.equal(getState(), other.getState())
                && Objects.equal(getPath(), other.getPath())
                && Objects.equal(getZxid(), other.getZxid());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getZxid(), getType(), getState(), getPath());
    }
}
