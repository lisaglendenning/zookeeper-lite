package edu.uw.zookeeper.data;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import net.engio.mbassy.listener.Handler;

import org.apache.zookeeper.Watcher;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.ZNodeLabel.Component;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.data.ZNodeLabelTrie.SimplePointer;

public class WatchPromiseTrie implements Reference<ZNodeLabelTrie<WatchPromiseTrie.WatchPromiseNode>> {

    public static WatchPromiseTrie newInstance() {
        return new WatchPromiseTrie();
    }
    
    protected final Logger logger;
    protected final ZNodeLabelTrie<WatchPromiseNode> trie;
    
    protected WatchPromiseTrie() {
        this.logger = LogManager.getLogger(getClass());
        this.trie = ZNodeLabelTrie.of(WatchPromiseNode.root());
    }
    
    public ListenableFuture<WatchEvent> subscribe(ZNodeLabel.Path path, EnumSet<Watcher.Event.EventType> types) {
        WatchPromiseNode node = get().root().add(path);
        return node.subscribe(types);
    }

    public ListenableFuture<WatchEvent> unsubscribe(ZNodeLabel.Path path, EnumSet<Watcher.Event.EventType> types) {
        ListenableFuture<WatchEvent> watch = null;
        WatchPromiseNode node = get().get(path);
        if (node != null) {
            watch = node.unsubscribe(types);
        }
        return watch;
    }
    
    @Override
    public ZNodeLabelTrie<WatchPromiseNode> get() {
        return trie;
    }

    @Handler
    public void handleWatchEvent(WatchEvent event) {
        WatchPromiseNode node = get().get(event.getPath());
        if (node != null) {
            List<Promise<WatchEvent>> watches = node.notify(event);
            if (watches.isEmpty()) {
                logger.debug("No watches registered for event {}", event);
            }
        }
    }

    public static class WatchPromiseNode extends ZNodeLabelTrie.DefaultsNode<WatchPromiseNode> {
    
        public static WatchPromiseNode root() {
            return new WatchPromiseNode(Optional.<ZNodeLabelTrie.Pointer<WatchPromiseNode>>absent());
        }
    
        protected final Map<EnumSet<Watcher.Event.EventType>, Promise<WatchEvent>> registry;
        
        protected WatchPromiseNode(
                Optional<Pointer<WatchPromiseNode>> parent) {
            super(parent);
            this.registry = Collections.synchronizedMap(Maps.<EnumSet<Watcher.Event.EventType>, Promise<WatchEvent>>newHashMap());
        }
        
        public ListenableFuture<WatchEvent> subscribe(EnumSet<Watcher.Event.EventType> types) {
            Promise<WatchEvent> watch = registry.get(types);
            if (watch == null) {
                synchronized (registry) {
                    if (registry.containsKey(types)) {
                        watch = registry.get(types);
                    } else {
                        watch = SettableFuturePromise.create();
                        registry.put(types, watch);
                    }
                }
            }
            return watch;
        }
    
        public ListenableFuture<WatchEvent> unsubscribe(EnumSet<Watcher.Event.EventType> types) {
            return registry.remove(types);
        }
        
        public List<Promise<WatchEvent>> notify(WatchEvent event) {
            List<Promise<WatchEvent>> watches = Lists.newLinkedList();
            synchronized (registry) {
                for (EnumSet<Watcher.Event.EventType> types: registry.keySet()) {
                    if (types.contains(event.getType())) {
                        Promise<WatchEvent> promise = registry.remove(types);
                        if (promise != null) {
                            watches.add(promise);
                        }
                    }
                }
            }
            for (Promise<WatchEvent> watch: watches) {
                watch.set(event);
            }
            return watches;
        }
    
        @Override
        protected WatchPromiseNode newChild(Component label) {
            Pointer<WatchPromiseNode> pointer = SimplePointer.of(label, this);
            return new WatchPromiseNode(Optional.of(pointer));
        }
    }
}
