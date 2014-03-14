
package edu.uw.zookeeper.data;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Objects;

public final class KeeperWatchEvent extends WatchEvent {

    public static KeeperWatchEvent of(KeeperState keeperState) {
        return new KeeperWatchEvent(keeperState);
    }
    
    private final KeeperState keeperState;
    
    public KeeperWatchEvent(KeeperState keeperState) {
        this.keeperState = keeperState;
    }
    
    @Override
    public KeeperState getKeeperState() {
        return keeperState;
    }

    @Override
    public EventType getEventType() {
        return EventType.None;
    }

    @Override
    public ZNodePath getPath() {
        return ZNodePath.root();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("keeperState", getKeeperState()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof KeeperWatchEvent)) {
            return false;
        }
        KeeperWatchEvent other = (KeeperWatchEvent) obj;
        return Objects.equal(getKeeperState(), other.getKeeperState());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(getKeeperState());
    }
}
