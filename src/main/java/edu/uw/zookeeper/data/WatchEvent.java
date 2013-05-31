
package edu.uw.zookeeper.data;

import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Event;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

@Event
public class WatchEvent implements Operation.RecordHolder<IWatcherEvent> {
    
    public static WatchEvent of(WatcherEvent message) {
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
    
    protected WatchEvent(EventType eventType, KeeperState keeperState, ZNodeLabel.Path path) {
        this.keeperState = keeperState;
        this.eventType = eventType;
        this.path = path;
    }
    
    public KeeperState state() {
        return keeperState;
    }
    
    public EventType type() {
        return eventType;
    }
    
    public ZNodeLabel.Path path() {
        return path;
    }

    @Override
    public IWatcherEvent asRecord() {
        return new IWatcherEvent(type().getIntValue(), 
                state().getIntValue(), 
                path().toString());
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("path", path).add("type", type()).add("state", state()).toString();
    }
}
