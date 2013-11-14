package edu.uw.zookeeper.data;

import java.util.EnumSet;
import java.util.concurrent.CopyOnWriteArraySet;

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

public class WatchListenerTrie implements SessionListener, Eventful<WatchListenerTrie.WatchMatchListener> {

    public static class WatchMatcher {
        
        public static enum PathMatchType {
            EXACT, PREFIX;
        }
        
        private final ZNodeLabel.Path path;
        private final PathMatchType pathType;
        private final EnumSet<Watcher.Event.EventType> eventType;
        
        public WatchMatcher(
                ZNodeLabel.Path path, 
                PathMatchType pathType,
                EnumSet<Watcher.Event.EventType> eventType) {
            super();
            this.path = path;
            this.pathType = pathType;
            this.eventType = eventType;
        }

        public ZNodeLabel.Path getPath() {
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
    
    public static WatchListenerTrie newInstance() {
        return new WatchListenerTrie(
                WatchListenerNode.root());
    }
    
    protected final DefaultsZNodeLabelTrie<WatchListenerNode> watchers;
    
    protected WatchListenerTrie(
            WatchListenerNode root) {
        this.watchers = DefaultsZNodeLabelTrie.of(root);
    }
    
    @Override
    public void subscribe(WatchMatchListener listener) {
        watchers.putIfAbsent(listener.getWatchMatcher().getPath(), new SubscribeVisitor(listener));
    }

    @Override
    public boolean unsubscribe(WatchMatchListener listener) {
        return watchers.get(listener.getWatchMatcher().getPath(), new UnsubscribeVisitor(listener));
    }

    @Override
    public void handleNotification(Operation.ProtocolResponse<IWatcherEvent> message) {
        WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.record());
        watchers.longestPrefix(event.getPath(), new WatchEventVisitor(event));
    }

    @Override
    public void handleAutomatonTransition(Automaton.Transition<ProtocolState> transition) {
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
    
    public static class SubscribeVisitor implements Function<WatchListenerNode, Void> {
        
        private final WatchMatchListener listener;
        
        public SubscribeVisitor(WatchMatchListener listener) {
            this.listener = listener;
        }
        
        @Override
        public Void apply(WatchListenerNode input) {
            input.subscribe(listener);
            return null;
        }
    }

    public static class UnsubscribeVisitor implements Function<WatchListenerNode, Boolean> {
        
        private final WatchMatchListener listener;
        
        public UnsubscribeVisitor(WatchMatchListener listener) {
            this.listener = listener;
        }
        
        @Override
        public Boolean apply(WatchListenerNode input) {
            Boolean unsubscribed;
            if (input != null) { 
                unsubscribed = Boolean.valueOf(input.unsubscribe(listener));
                WatchListenerNode node = input;
                while ((node != null) && !node.subscribed() && node.isEmpty()) {
                    WatchListenerNode parent = node.parent().get();
                    node.remove();
                    node = parent;
                }
            } else {
                unsubscribed = Boolean.FALSE;
            }
            return unsubscribed;
        }
    }
    
    public static class WatchListenerNode extends DefaultsZNodeLabelTrie.AbstractDefaultsNode<WatchListenerNode> implements Eventful<WatchListenerTrie.WatchMatchListener>, WatchListener {
    
        public static WatchListenerNode root() {
            ZNodeLabelTrie.Pointer<WatchListenerNode> pointer = ZNodeLabelTrie.strongPointer(ZNodeLabel.none(), null);
            return new WatchListenerNode(pointer);
        }

        protected final CopyOnWriteArraySet<WatchMatchListener> listeners;
        
        protected WatchListenerNode(
                ZNodeLabelTrie.Pointer<WatchListenerNode> parent) {
            super(ZNodeLabelTrie.pathOf(parent), 
                    parent, 
                    Maps.<ZNodeLabel.Component, WatchListenerNode>newHashMap());
            this.listeners = Sets.newCopyOnWriteArraySet();
        }

        @Override
        public void subscribe(WatchMatchListener listener) {
            listeners.add(listener);
        }

        @Override
        public boolean unsubscribe(WatchMatchListener listener) {
            return listeners.remove(listener);
        }
        
        public boolean subscribed() {
            return listeners.isEmpty();
        }

        @Override
        public void handleWatchEvent(WatchEvent event) {
            for (WatchMatchListener listener: listeners) {
                if ((path().equals(event.getPath()) || (listener.getWatchMatcher().getPathType() == WatchMatcher.PathMatchType.PREFIX))
                        && listener.getWatchMatcher().getEventType().contains(event.getType())) {
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
        protected WatchListenerNode newChild(ZNodeLabel.Component label) {
            ZNodeLabelTrie.Pointer<WatchListenerNode> pointer = ZNodeLabelTrie.weakPointer(label, this);
            return new WatchListenerNode(pointer);
        }
    }
}
