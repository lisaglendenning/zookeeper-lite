
package edu.uw.zookeeper.data;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public final class NodeWatchEvent extends WatchEvent {

    public static NodeWatchEvent nodeCreated(ZNodePath path) {
        return of(path, EventType.NodeCreated);
    }

    public static NodeWatchEvent nodeDeleted(ZNodePath path) {
        return of(path, EventType.NodeDeleted);
    }

    public static NodeWatchEvent nodeDataChanged(ZNodePath path) {
        return of(path, EventType.NodeDataChanged);
    }

    public static NodeWatchEvent nodeChildrenChanged(ZNodePath path) {
        return of(path, EventType.NodeChildrenChanged);
    }
    
    public static NodeWatchEvent of(ZNodePath path, EventType eventType) {
        return new NodeWatchEvent(path, eventType);
    }
    
    private final ZNodePath path;
    private final EventType eventType;
    
    public NodeWatchEvent(ZNodePath path, EventType eventType) {
        this.eventType = eventType;
        this.path = path;
    }

    @Override
    public KeeperState getKeeperState() {
        return KeeperState.SyncConnected;
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }

    @Override
    public ZNodePath getPath() {
        return path;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("path", getPath()).add("eventType", getEventType()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof NodeWatchEvent)) {
            return false;
        }
        NodeWatchEvent other = (NodeWatchEvent) obj;
        return Objects.equal(getEventType(), other.getEventType())
                && Objects.equal(getPath(), other.getPath());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getEventType(), getPath());
    }
}
