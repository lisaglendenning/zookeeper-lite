package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
        while (remaining.hasNext()) {
            next = remaining.next();
            schemaNode = ZNodeSchema.matchChild(schemaNode, next);
            node = node.putIfAbsent(schemaNode.parent().name());
        }
        node.get().put(next, listener);
    }

    @Override
    public synchronized boolean unsubscribe(final WatchMatchListener listener) {
        final ZNodePath path = listener.getWatchMatcher().getPath();
        final Iterator<ZNodeLabel> remaining = path.iterator();
        WatchListenersNode node = listeners.root();
        ValueNode<ZNodeSchema> schemaNode = schema.root();
        AbstractZNodeLabel next = EmptyZNodeLabel.getInstance();
        while ((node != null) && remaining.hasNext()) {
            next = remaining.next();
            schemaNode = ZNodeSchema.matchChild(schemaNode, next);
            node = node.get(schemaNode.parent().name());
        }
        if (node != null) { 
            if (node.get().remove(next, listener)) {
                // garbage collect unused nodes
                while ((node != null) && node.isEmpty() && node.get().isEmpty() && node.remove()) {
                    node = node.parent().get();
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
            // avoid ConcurrentModificationException
            for (AbstractZNodeLabel k: ImmutableSet.copyOf(node.get().keySet())) {
                CopyOnWriteArraySet<WatchMatchListener> v = node.get().get(k);
                if (v != null) {
                    for (WatchMatchListener listener: v) {
                        listener.handleAutomatonTransition(transition);
                    }
                }
            }
        }
    }
    
    @Override
    public synchronized void handleWatchEvent(final WatchEvent event) {
        final ZNodePath path = event.getPath();
        final Iterator<ZNodeLabel> remaining = path.iterator();
        WatchListenersNode node = listeners.root();
        AbstractZNodeLabel next = EmptyZNodeLabel.getInstance();
        List<CopyOnWriteArraySet<WatchMatchListener>> matching = Lists.newArrayListWithCapacity(2);
        while (node != null) {
            // dispatch to exact label and the pattern label
            CopyOnWriteArraySet<WatchMatchListener> listeners = node.get().get(next);
            if (listeners != null) {
                matching.add(listeners);
            }
            if (!node.parent().name().equals(next)) {
                listeners = node.get().get((AbstractZNodeLabel) node.parent().name());
                if (listeners != null) {
                    matching.add(listeners);
                }
            }
            // we don't iterate over map to avoid ConcurrentModificationException
            for (CopyOnWriteArraySet<WatchMatchListener> v: matching) {
                for (WatchMatchListener listener: v) {
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
            matching.clear();
        }
    }
    
    /**
     * Not thread safe.
     */
    protected static final class MultiCopyOnWriteSet<K,V> {

        public static <K,V> MultiCopyOnWriteSet<K,V> newHashMap() {
            return new MultiCopyOnWriteSet<K,V>(Maps.<K,CopyOnWriteArraySet<V>>newHashMap());
        }
        
        // we use CopyOnWriteArray just to avoid ConcurrentModificationException
        // if a listener unsubscribes itself while handling an event
        // plus, we expect subscription changes to be rare compared to
        // event handling
        // but we still need to be careful about iterating over the map itself
        protected final Map<K, CopyOnWriteArraySet<V>> delegate;
        
        protected MultiCopyOnWriteSet(Map<K, CopyOnWriteArraySet<V>> delegate) {
            this.delegate = delegate;
        }
        
        public boolean isEmpty() {
            return delegate.isEmpty();
        }
        
        public Set<K> keySet() {
            return delegate.keySet();
        }
        
        public CopyOnWriteArraySet<V> get(K k) {
            return delegate.get(k);
        }
        
        public boolean put(K k, V v) {
            return putIfAbsent(k).add(v);
        }
        
        public CopyOnWriteArraySet<V> putIfAbsent(K k) {
            CopyOnWriteArraySet<V> set = delegate.get(k);
            if (set == null) {
                set = Sets.newCopyOnWriteArraySet();
                delegate.put(k, set);
            }
            return set;
        }
        
        public boolean remove(K k, V v) {
            CopyOnWriteArraySet<V> listeners = delegate.get(k);
            if ((listeners != null) && listeners.remove(v)) {
                if (listeners.isEmpty()) {
                    delegate.remove(k);
                }
                return true;
            }
            return false;
        }
    }
    
    protected static final class WatchListenersNode extends DefaultsNode.AbstractDefaultsNode<WatchListenersNode> implements Supplier<MultiCopyOnWriteSet<AbstractZNodeLabel, WatchMatchListener>> {
    
        public static WatchListenersNode root() {
            NameTrie.Pointer<WatchListenersNode> pointer = SimpleLabelTrie.<WatchListenersNode>rootPointer();
            return new WatchListenersNode(pointer);
        }

        protected final MultiCopyOnWriteSet<AbstractZNodeLabel, WatchMatchListener> value;
        
        protected WatchListenersNode(
                NameTrie.Pointer<WatchListenersNode> parent) {
            super(parent);
            this.value = MultiCopyOnWriteSet.newHashMap();
        }
        
        @Override
        public MultiCopyOnWriteSet<AbstractZNodeLabel, WatchMatchListener> get() {
            return value;
        }

        @Override
        protected WatchListenersNode newChild(final ZNodeName name) {
            final NameTrie.Pointer<WatchListenersNode> pointer = SimpleLabelTrie.weakPointer(name, this);
            return new WatchListenersNode(pointer);
        }
    }
}
