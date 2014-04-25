package edu.uw.zookeeper.data;


import static com.google.common.base.Preconditions.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.jute.Record;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Eventful;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.SimpleLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.ZNodeName;
import edu.uw.zookeeper.data.NameTrie;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.SessionListener;
import edu.uw.zookeeper.protocol.ZxidReference;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IStat;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpResponse;

/**
 * Only caches the results of operations submitted through this wrapper.
 * 
 * Not thread-safe.
 */
public class ZNodeCache<E extends AbstractNameTrie.SimpleNode<E> & ZNodeCache.CacheNode<E,?>, I extends Operation.Request, O extends Operation.ProtocolResponse<?>> 
        implements ClientExecutor<I, O, SessionListener> {

    public static <I extends Operation.Request,O extends Operation.ProtocolResponse<?>> ZNodeCache<SimpleCacheNode,I,O> newInstance(
            ClientExecutor<? super I,O,SessionListener> client) {
        return newInstance(client, SimpleCacheNode.root());
    }
    
    public static <E extends AbstractNameTrie.SimpleNode<E> & ZNodeCache.CacheNode<E,?>,I extends Operation.Request, O extends Operation.ProtocolResponse<?>> ZNodeCache<E,I,O> newInstance(
            ClientExecutor<? super I,O,SessionListener> client, E root) {
        return new ZNodeCache<E,I,O>(client, new CacheEvents(new StrongConcurrentSet<CacheListener>()), SimpleLabelTrie.forRoot(root));
    }

    public static interface CacheListener {
        void handleCacheEvent(Set<NodeWatchEvent> events);
    }
    
    public static interface CacheNode<E extends CacheNode<E,?>, V> extends DefaultsNode<E> {

        long stamp();
        
        long touch(long zxid);
        
        List<NodeWatchEvent> update(long zxid, Record...records);
        
        StampedValue<Records.ZNodeStatGetter> stat();
        
        StampedValue<V> data();
    }
    
    public static class CacheEvents implements Eventful<CacheListener>, CacheListener {

        protected final IConcurrentSet<CacheListener> listeners;
        
        public CacheEvents(IConcurrentSet<CacheListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void subscribe(CacheListener listener) {
            listeners.add(listener);
        }

        @Override
        public boolean unsubscribe(CacheListener listener) {
            return listeners.remove(listener);
        }

        @Override
        public void handleCacheEvent(Set<NodeWatchEvent> events) {
            if (! events.isEmpty()) {
                for (CacheListener listener: listeners) {
                    listener.handleCacheEvent(events);
                }
            }
        }
    }
    
    public static abstract class AbstractCacheNode<E extends AbstractCacheNode<E,?>, V> extends DefaultsNode.AbstractDefaultsNode<E> implements CacheNode<E,V> {

        protected long stamp;
        protected StampedValue<Records.ZNodeStatGetter> stat;
        protected StampedValue<V> data;

        protected AbstractCacheNode(
                NameTrie.Pointer<? extends E> parent) {
            this(null, null, -1L, parent);
        }

        protected AbstractCacheNode(
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends E> parent) {
            this(data, stat, stamp, parent, Maps.<ZNodeName, E>newHashMap());
        }

        protected AbstractCacheNode(
                V data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends E> parent,
                Map<ZNodeName, E> children) {
            super(SimpleLabelTrie.pathOf(parent), parent, children);
            this.stat = StampedValue.valueOf(stamp, stat);
            this.data = StampedValue.valueOf(stamp, data);
            this.stamp = stamp;
        }
        
        @Override
        public long stamp() {
            return stamp;
        }
        
        @Override
        public long touch(long stamp) {
            if (this.stamp < stamp) {
                long prev = stamp;
                this.stamp = stamp;
                return prev;
            } else {
                return this.stamp;
            }
        }
        
        @Override
        public StampedValue<Records.ZNodeStatGetter> stat() {
            return stat;
        }

        @Override
        public StampedValue<V> data() {
            return data;
        }
        
        @Override
        public List<NodeWatchEvent> update(long zxid, Record...records) {
            ImmutableList<NodeWatchEvent> events = ImmutableList.of();
            touch(zxid);
            for (Record record: records) {
                if (record instanceof Records.StatGetter) {
                    if (zxid > stat.stamp()) {
                        Records.ZNodeStatGetter prev = stat.get();
                        Records.ZNodeStatGetter updated = new IStat(((Records.StatGetter) record).getStat());
                        if (! Objects.equal(prev, updated)) {
                            stat = StampedValue.valueOf(zxid, updated);
                        }
                    }
                }
                if (record instanceof Records.DataGetter) {
                    if (zxid > stat.stamp()) {
                        V prev = data.get();
                        V updated = transformData(((Records.DataGetter) record).getData());
                        if (! equivalentData(prev, updated)) {
                            data = StampedValue.valueOf(zxid, updated);
                            events = ImmutableList.of(NodeWatchEvent.nodeDataChanged(path()));
                        }
                    }
                }
            }
            return events;
        }
        
        protected abstract V transformData(byte[] data);
        
        protected abstract boolean equivalentData(V v1, V v2);

        @Override
        public String toString() {
            return Objects.toStringHelper("")
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp())
                    .add("stat", stat())
                    .add("data", data()).toString();
        }
    }

    public static class SimpleCacheNode extends AbstractCacheNode<SimpleCacheNode, byte[]> {

        public static SimpleCacheNode root() {
            return new SimpleCacheNode(SimpleLabelTrie.<SimpleCacheNode>rootPointer());
        }
        

        protected SimpleCacheNode(
                NameTrie.Pointer<? extends SimpleCacheNode> parent) {
            super(parent);
        }
        
        protected SimpleCacheNode(
                byte[] data,
                Records.ZNodeStatGetter stat,
                long stamp,
                NameTrie.Pointer<? extends SimpleCacheNode> parent) {
            super(data, stat, stamp, parent);
        }

        @Override
        protected byte[] transformData(byte[] data) {
            return data;
        }

        @Override
        protected boolean equivalentData(byte[] v1, byte[] v2) {
            return Arrays.equals(v1, v2);
        }
        
        @Override
        protected SimpleCacheNode newChild(ZNodeName label) {
            NameTrie.Pointer<SimpleCacheNode> pointer = SimpleLabelTrie.weakPointer(label, this);
            return new SimpleCacheNode(pointer);
        }
    }

    // wrapper so that we can apply changes to the cache before
    // listeners are notified
    protected class PromiseWrapper extends PromiseTask<I,O> {

        protected PromiseWrapper(I task) {
            this(task, SettableFuturePromise.<O>create());
        }

        protected PromiseWrapper(I task, Promise<O> delegate) {
            super(task, delegate);
        }
        
        @Override
        public boolean set(O result) {
            if (! isDone()) {
                Records.Request request = (Records.Request)
                        ((task() instanceof Records.Request) ?
                                task() :
                                    ((Operation.RecordHolder<?>) task()).record());
                handleResult(request, result);
            }
            return super.set(result);
        }
        
        @Override
        protected Promise<O> delegate() {
            return delegate;
        }
    }
    
    protected final Logger logger;
    protected final ZxidTracker lastZxid;
    protected final ClientExecutor<? super I, O, SessionListener> client;
    protected final NameTrie<E> trie;
    protected final CacheEvents events;
    
    protected ZNodeCache( 
            ClientExecutor<? super I, O, SessionListener> client,
            CacheEvents events,
            NameTrie<E> trie) {
        this.logger = LogManager.getLogger(getClass());
        this.client = checkNotNull(client);
        this.events = checkNotNull(events);
        this.lastZxid = ZxidTracker.create();
        this.trie = checkNotNull(trie);
    }
    
    public NameTrie<E> cache() {
        return trie;
    }
    
    public CacheEvents events() {
        return events;
    }
    
    public ZxidReference lastZxid() {
        return lastZxid;
    }
    
    public ClientExecutor<? super I, O, SessionListener> client() {
        return client;
    }
    
    @Override
    public void subscribe(SessionListener listener) {
        client.subscribe(listener);
    }

    @Override
    public boolean unsubscribe(SessionListener listener) {
        return client.unsubscribe(listener);
    }

    @Override
    public ListenableFuture<O> submit(I request) {
        return client.submit(request, new PromiseWrapper(request));
    }
    
    @Override
    public ListenableFuture<O> submit(I request, Promise<O> promise) {
        return client.submit(request, new PromiseWrapper(request, promise));
    }
    
    protected void handleResult(Records.Request request, Operation.ProtocolResponse<?> result) {
        long zxid = result.zxid();
        lastZxid.update(zxid);
        Records.Response response = result.record();
        ImmutableSet.Builder<NodeWatchEvent> events = ImmutableSet.builder();
        if (response instanceof Operation.Error) {
            switch (((Operation.Error) response).error()) {
            case NONODE:
            {
                ZNodePath path = ZNodePath.fromString(((Records.PathGetter) request).getPath());
                switch (request.opcode()) {
                case CREATE:
                case CREATE2:
                {
                    path = ((AbsoluteZNodePath) path).parent();
                }
                case CHECK:
                case DELETE:
                case EXISTS:
                case GET_ACL:
                case GET_CHILDREN:
                case GET_CHILDREN2:
                case GET_DATA:
                case SET_ACL:
                case SET_DATA:
                {
                    remove(path, zxid, events);
                    return;
                }
                default:
                    break;
                }
                break;
            }
            case NODEEXISTS:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                add(path, zxid, events);    
                break;
            }
            default:
                break;
            }
        } else {
            switch (response.opcode()) {
            case CREATE:
            case CREATE2:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) response).getPath());
                update(path, zxid, events, request, response);
                break;
            }
            case DELETE:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                remove(path, zxid, events);
                break;
            }
            case CHECK:
            case EXISTS:
            case GET_ACL:
            case SET_ACL:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                update(path, zxid, events, response);
                break;
            }
            case GET_CHILDREN:
            case GET_CHILDREN2:        
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                List<String> children = ((Records.ChildrenGetter) response).getChildren();
                E node = add(path, zxid, events);
                for (Map.Entry<ZNodeName, E> entry: node.entrySet()) {
                    if (! children.contains(entry.getKey().toString())) {
                        remove(entry.getValue().path(), zxid, events);
                    }
                }
                for (String child: children) {
                    add(path.join(ZNodeLabel.fromString(child)), zxid, events);
                }
                update(path, zxid, events, response);
                break;
            }
            case GET_DATA:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                update(path, zxid, events, response);
                break;
            }
            case MULTI:
            {
                int xid = result.xid();
                IMultiRequest requestRecord = (IMultiRequest) request;
                IMultiResponse responseRecord = (IMultiResponse) response;
                Iterator<MultiOpRequest> requests = requestRecord.iterator();
                Iterator<MultiOpResponse> responses = responseRecord.iterator();
                while (requests.hasNext()) {
                     handleResult(requests.next(),
                            ProtocolResponseMessage.of(xid, zxid, responses.next()));
                }
                break;
            }
            case SET_DATA:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                update(path, zxid, events, response);
                break;
            }
            default:
                break;
            }
        }
        
        events().handleCacheEvent(events.build());
    }

    protected E add(ZNodePath path, long stamp, ImmutableSet.Builder<NodeWatchEvent> events) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        E node = trie.root();
        while (remaining.hasNext()) {
            if (node.touch(stamp) < 0L) {
                events.add(NodeWatchEvent.nodeCreated(node.path()));
                if (! node.path().isRoot()) {
                    events.add(NodeWatchEvent.nodeChildrenChanged(node.parent().get().path()));
                }
            }
            node = node.putIfAbsent(remaining.next());
        }
        return node;
    }

    protected E remove(ZNodePath path, long stamp, ImmutableSet.Builder<NodeWatchEvent> events) {
        E node = trie.get(path);
        if ((node != null) && (node.stamp() < stamp)) {
            if (node.remove()) {
                events.add(NodeWatchEvent.nodeDeleted(node.path()));
                events.add(NodeWatchEvent.nodeChildrenChanged((node.parent().get().path())));
                return node;
            }
        }
        return null;
    }

    protected E update(ZNodePath path, long stamp, ImmutableSet.Builder<NodeWatchEvent> events, Record...records) {
        E node = add(path, stamp, events);
        events.addAll(node.update(stamp, records));
        return node;
    }
}
