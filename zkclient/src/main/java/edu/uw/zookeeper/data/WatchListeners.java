package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;

import edu.uw.zookeeper.common.Automaton;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolState;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;

public class WatchListeners implements SessionListener, Eventful<WatchMatchListener>, WatchListener {

    public static WatchListeners newInstance(NameTrie<ValueNode<ZNodeSchema>> schema) {
        return new WatchListeners(
                checkNotNull(schema),
                SimpleLabelTrie.forRoot(WatchListenersNode.root()));
    }
    
    protected final NameTrie<ValueNode<ZNodeSchema>> schema;
    protected final NameTrie<WatchListenersNode> listeners;
    
    protected WatchListeners(
            NameTrie<ValueNode<ZNodeSchema>> schema,
            NameTrie<WatchListenersNode> listeners) {
        this.schema = schema;
        this.listeners = listeners;
    }
    
    @Override
    public synchronized void subscribe(final WatchMatchListener listener) {
        final ZNodePath path = listener.getWatchMatcher().getPath();
        final Iterator<ZNodeLabel> remaining = path.iterator();
        WatchListenersNode node = listeners.root();
        ValueNode<ZNodeSchema> schemaNode = schema.root();
        AbstractZNodeLabel next = EmptyZNodeLabel.getInstance();
        while (node != null) {
            if (remaining.hasNext()) {
                next = remaining.next();
                schemaNode = ZNodeSchema.matchChild(schemaNode, next);
                node = node.putIfAbsent(schemaNode.parent().name());
            } else {
                node.listeners().put(next, listener);
                node = null;
            }
        }
    }

    @Override
    public synchronized boolean unsubscribe(final WatchMatchListener listener) {
        final ZNodePath path = listener.getWatchMatcher().getPath();
        WatchListenersNode node = listeners.get(path);
        if (node != null) { 
            if (node.listeners().remove(path.label(), listener)) {
                // garbage collect unused nodes
                while ((node != null) && node.isEmpty() && node.listeners().isEmpty()) {
                    WatchListenersNode parent = node.parent().get();
                    if (parent != null) {
                        parent.remove(node.parent().name());
                    }
                    node = parent;
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void handleNotification(
            final Operation.ProtocolResponse<IWatcherEvent> message) {
        final WatchEvent event = WatchEvent.fromRecord((IWatcherEvent) message.record());
        handleWatchEvent(event);
    }

    @Override
    public synchronized void handleAutomatonTransition(
            final Automaton.Transition<ProtocolState> transition) {
        for (WatchListenersNode node: listeners) {
            for (WatchMatchListener listener: node.listeners().values()) {
                listener.handleAutomatonTransition(transition);
            }
        }
    }
    
    @Override
    public synchronized void handleWatchEvent(final WatchEvent event) {
        final ZNodePath path = event.getPath();
        final Iterator<ZNodeLabel> remaining = path.iterator();
        WatchListenersNode node = listeners.root();
        AbstractZNodeLabel next = EmptyZNodeLabel.getInstance();
        while (node != null) {
            // dispatch to exact label and the pattern label
            List<AbstractZNodeLabel> labels; 
            if (node.parent().name().equals(next)) {
                labels = ImmutableList.of(next); 
            } else {
                labels = ImmutableList.of(next, (ZNodeLabel) node.parent().name());
            }
            for (AbstractZNodeLabel label: labels) {
                for (WatchMatchListener listener: node.listeners().get(label)) {
                    if (listener.getWatchMatcher().getEventType().contains(event.getEventType())
                            && ((listener.getWatchMatcher().getPathType() == WatchMatcher.PathMatchType.PREFIX) 
                                    || !remaining.hasNext())) {
                        listener.handleWatchEvent(event);
                    }
                }
            }
            
            if (remaining.hasNext()) {
                next = remaining.next();
                node = ZNodeSchema.matchChild(node, next);
            } else {
                node = null;
            }
        }
    }
    
    protected static final class WatchListenersNode extends DefaultsNode.AbstractDefaultsNode<WatchListenersNode> {
    
        public static WatchListenersNode root() {
            NameTrie.Pointer<WatchListenersNode> pointer = SimpleLabelTrie.<WatchListenersNode>rootPointer();
            return new WatchListenersNode(pointer);
        }

        protected final SetMultimap<AbstractZNodeLabel, WatchMatchListener> listeners;
        
        protected WatchListenersNode(
                NameTrie.Pointer<WatchListenersNode> parent) {
            super(parent);
            this.listeners = HashMultimap.create();
        }
        
        public SetMultimap<AbstractZNodeLabel, WatchMatchListener> listeners() {
            return listeners;
        }

        @Override
        protected WatchListenersNode newChild(final ZNodeName name) {
            final NameTrie.Pointer<WatchListenersNode> pointer = SimpleLabelTrie.weakPointer(name, this);
            return new WatchListenersNode(pointer);
        }
    }
}
