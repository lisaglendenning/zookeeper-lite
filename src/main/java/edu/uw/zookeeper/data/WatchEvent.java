
package edu.uw.zookeeper.data;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Event;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;

@Event
public class WatchEvent {

    public static WatchEvent created(ZNodeLabel.Path path) {
        return of(EventType.NodeCreated, KeeperState.SyncConnected, path);
    }

    public static WatchEvent deleted(ZNodeLabel.Path path) {
        return of(EventType.NodeDeleted, KeeperState.SyncConnected, path);
    }

    public static WatchEvent data(ZNodeLabel.Path path) {
        return of(EventType.NodeDataChanged, KeeperState.SyncConnected, path);
    }

    public static WatchEvent children(ZNodeLabel.Path path) {
        return of(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
    }
    
    public static WatchEvent valueOf(IWatcherEvent message) {
        return new WatchEvent(EventType.fromInt(message.getType()), 
                KeeperState.fromInt(message.getState()),
                ZNodeLabel.Path.of(message.getPath()));
    }

    public static WatchEvent of(EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        return new WatchEvent(eventType, keeperState, path);
    }
    
    private final ZNodeLabel.Path path;
    private final KeeperState keeperState;
    private final EventType eventType;
    
    public WatchEvent(EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
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

    public IWatcherEvent asRecord() {
        return new IWatcherEvent(
                getType().getIntValue(), 
                getState().getIntValue(), 
                getPath().toString());
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
                && Objects.equal(getPath(), other.getPath());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getType(), getState(), getPath());
    }
}
