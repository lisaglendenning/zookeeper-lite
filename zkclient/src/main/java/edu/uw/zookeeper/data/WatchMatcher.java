package edu.uw.zookeeper.data;

import java.util.EnumSet;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Objects;

public final class WatchMatcher {

    public static WatchMatcher exact(ZNodePath path, Watcher.Event.EventType first, Watcher.Event.EventType...rest) {
        return exact(path, EnumSet.of(first, rest));
    }

    public static WatchMatcher exact(ZNodePath path, Watcher.Event.EventType first) {
        return exact(path, EnumSet.of(first));
    }

    public static WatchMatcher exact(ZNodePath path, EnumSet<Watcher.Event.EventType> eventType) {
        return new WatchMatcher(path, PathMatchType.EXACT, eventType);
    }

    public static WatchMatcher prefix(ZNodePath path, Watcher.Event.EventType first) {
        return prefix(path, EnumSet.of(first));
    }

    public static WatchMatcher prefix(ZNodePath path, Watcher.Event.EventType first, Watcher.Event.EventType...rest) {
        return prefix(path, EnumSet.of(first, rest));
    }

    public static WatchMatcher prefix(ZNodePath path, EnumSet<Watcher.Event.EventType> eventType) {
        return new WatchMatcher(path, PathMatchType.PREFIX, eventType);
    }
    
    public static enum PathMatchType {
        EXACT, PREFIX;
    }
    
    private final ZNodePath path;
    private final WatchMatcher.PathMatchType pathType;
    private final EnumSet<Watcher.Event.EventType> eventType;
    
    public WatchMatcher(
            ZNodePath path, 
            WatchMatcher.PathMatchType pathType,
            EnumSet<Watcher.Event.EventType> eventType) {
        super();
        this.path = path;
        this.pathType = pathType;
        this.eventType = eventType;
    }

    public ZNodePath getPath() {
        return path;
    }

    public WatchMatcher.PathMatchType getPathType() {
        return pathType;
    }
    
    public EnumSet<Watcher.Event.EventType> getEventType() {
        return eventType;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("path", path)
                .add("pathType", pathType)
                .add("eventType", eventType).toString();
    }
}
