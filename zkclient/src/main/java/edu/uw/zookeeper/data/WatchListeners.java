package edu.uw.zookeeper.data;

import java.util.EnumSet;
import java.util.Set;

import org.apache.zookeeper.Watcher;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Automatons;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class WatchListeners implements SessionListener, Eventful<WatchListeners.WatchMatchListener> {

    public static class WatchMatcher {

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
        private final PathMatchType pathType;
        private final EnumSet<Watcher.Event.EventType> eventType;
        
        public WatchMatcher(
                ZNodePath path, 
                PathMatchType pathType,
                EnumSet<Watcher.Event.EventType> eventType) {
            super();
            this.path = path;
            this.pathType = pathType;
            this.eventType = eventType;
        }

        public ZNodePath getPath() {
            return path;
        }

        public PathMatchType getPathType() {
            return pathType;
        }
        
        public EnumSet<Watcher.Event.EventType> getEventType() {
            return eventType;
        }
    }

    public static interface WatchListener extends Automatons.AutomatonListener<ProtocolState> {
        void handleWatchEvent(WatchEvent event);
    }
    
    public static interface WatchMatchListener extends WatchListener {
        WatchMatcher getWatchMatcher();
    }
    
    public static WatchListeners newInstance() {
        return new WatchListeners(
                WatchListenerNode.root());
    }
    
    protected final NameTrie<WatchListenerNode> watchers;
    
    protected WatchListeners(
            WatchListenerNode root) {
        this.watchers = SimpleNameTrie.forRoot(root);
    }
    
    @Override
    public synchronized void subscribe(WatchMatchListener listener) {
        WatchListenerNode.putIfAbsent(watchers, listener.getWatchMatcher().getPath()).subscribe(listener);
    }

    @Override
    public synchronized boolean unsubscribe(WatchMatchListener listener) {
        WatchListenerNode node = watchers.get(listener.getWatchMatcher().getPath());
        if (node != null) { 
            boolean unsubscribed = node.unsubscribe(listener);
            // garbage collect unused nodes
            while ((node != null) && node.listeners().isEmpty() && node.isEmpty()) {
                WatchListenerNode parent = node.parent().get();
                if (parent != null) {
                    parent.remove(node.parent().name());
                }
                node = parent;
            }
            return unsubscribed;
        } else {
            return false;
        }
    }

    @Override
    public synchronized void handleNotification(Operation.ProtocolResponse<IWatcherEvent> message) {
        WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.record());
        for (WatchListenerNode node = SimpleNameTrie.longestPrefix(watchers, event.getPath());
                (node != null); node = node.parent().get()) {
            node.handleWatchEvent(event);
        }
    }

    @Override
    public synchronized void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
        for (WatchListenerNode e: watchers) {
            e.handleAutomatonTransition(transition);
        }
    }
    
    public static class WatchEventVisitor implements Function<WatchListenerNode, Void> {

        private final WatchEvent event;
        
        public WatchEventVisitor(WatchEvent event) {
            this.event = event;
        }
        
        @Override
        public Void apply(WatchListenerNode input) {
            input.handleWatchEvent(event);
            WatchListenerNode parent = input.parent().get();
            if (parent != null) {
                return apply(parent);
            } else {
                return null;
            }
        }
    }
    
    public static class WatchListenerNode extends DefaultsNode.AbstractDefaultsNode<WatchListenerNode> implements Eventful<WatchListeners.WatchMatchListener>, WatchListener {
    
        public static WatchListenerNode root() {
            NameTrie.Pointer<WatchListenerNode> pointer = SimpleNameTrie.<WatchListenerNode>rootPointer();
            return new WatchListenerNode(pointer);
        }

        protected final Set<WatchMatchListener> listeners;
        
        protected WatchListenerNode(
                NameTrie.Pointer<WatchListenerNode> parent) {
            super(SimpleNameTrie.pathOf(parent), 
                    parent, 
                    Maps.<ZNodeName, WatchListenerNode>newHashMap());
            this.listeners = Sets.newHashSet();
        }

        @Override
        public void subscribe(WatchMatchListener listener) {
            listeners.add(listener);
        }

        @Override
        public boolean unsubscribe(WatchMatchListener listener) {
            return listeners.remove(listener);
        }
        
        protected Set<WatchMatchListener> listeners() {
            return listeners;
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            for (WatchMatchListener listener: listeners) {
                if ((path().equals(event.getPath()) || (listener.getWatchMatcher().getPathType() == WatchMatcher.PathMatchType.PREFIX))
                        && listener.getWatchMatcher().getEventType().contains(event.getEventType())) {
                    listener.handleWatchEvent(event);
                }
            }
        }
    
        @Override
        public void handleAutomatonTransition(
                Automaton.Transition<ProtocolState> transition) {
            for (WatchMatchListener listener: listeners) {
                listener.handleAutomatonTransition(transition);
            }
        }

        @Override
        protected WatchListenerNode newChild(ZNodeName label) {
            NameTrie.Pointer<WatchListenerNode> pointer = SimpleNameTrie.weakPointer(label, this);
            return new WatchListenerNode(pointer);
        }
    }
}
