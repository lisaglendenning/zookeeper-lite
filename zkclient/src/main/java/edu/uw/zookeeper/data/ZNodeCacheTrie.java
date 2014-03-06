package edu.uw.zookeeper.data;


import static com.google.common.base.Preconditions.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.engio.mbassy.common.IConcurrentSet;
import net.engio.mbassy.common.StrongConcurrentSet;

import org.apache.jute.Record;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.SimpleNameTrie;
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
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpResponse;

/**
 * Only caches the results of operations submitted through this wrapper.
 */
public class ZNodeCacheTrie<E extends ZNodeCacheTrie.CachedNode<E>, I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
        extends SimpleNameTrie<E> implements ClientExecutor<I, V, ZNodeCacheTrie.CacheSessionListener<? super E>> {

    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> ZNodeCacheTrie<SimpleCachedNode,I,V> newInstance(
            ClientExecutor<I,V,SessionListener> client) {
        return newInstance(client, SimpleCachedNode.root());
    }
    
    public static <E extends ZNodeCacheTrie.CachedNode<E>, I extends Operation.Request, V extends Operation.ProtocolResponse<?>> ZNodeCacheTrie<E,I,V> newInstance(
            ClientExecutor<I,V,SessionListener> client, E root) {
        return new ZNodeCacheTrie<E,I,V>(client, new StrongConcurrentSet<CacheSessionListener<? super E>>(), root);
    }

    public static abstract class CacheEvent<E extends CachedNode<E>> {

        private final E node;
        
        protected CacheEvent(E node) {
            this.node = node;
        }        
        
        public E getNode() {
            return node;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("node", getNode()).toString();
        }
    }

    public static class NodeAddedCacheEvent<E extends CachedNode<E>> extends CacheEvent<E> {

        public static <E extends CachedNode<E>> NodeAddedCacheEvent<E> of(E node) {
            return new NodeAddedCacheEvent<E>(node);
        }
        
        public NodeAddedCacheEvent(E node) {
            super(node);
        }
    }

    public static class NodeRemovedCacheEvent<E extends CachedNode<E>> extends CacheEvent<E> {

        public static <E extends CachedNode<E>> NodeAddedCacheEvent<E> of(E node) {
            return new NodeAddedCacheEvent<E>(node);
        }
        
        public NodeRemovedCacheEvent(E node) {
            super(node);
        }
    }

    public static class NodeUpdatedCacheEvent<E extends CachedNode<E>> extends CacheEvent<E> {

        public static <E extends CachedNode<E>> NodeUpdatedCacheEvent<E> of(E node, ImmutableSet<Object> types) {
            return new NodeUpdatedCacheEvent<E>(node, types);
        }
        
        private final ImmutableSet<Object> types;
        
        public NodeUpdatedCacheEvent(E node, ImmutableSet<Object> types) {
            super(node);
            this.types = types;
        }
        
        public ImmutableSet<Object> getTypes() {
            return types;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("node", getNode())
                    .add("types", getTypes()).toString();
        }
    }
    
    public static interface CacheListener<E extends ZNodeCacheTrie.CachedNode<E>> {
        void handleCacheUpdate(ZNodeCacheTrie.CacheEvent<? extends E> event);
    }
    
    public static interface CacheSessionListener<E extends CachedNode<E>> extends CacheListener<E>, SessionListener {
    }
    
    public static interface CachedNode<E extends CachedNode<E>> extends DefaultsNode<E> {

        long stamp();
        
        long touch(long zxid);
        
        <T> StampedReference<T> getCached(Object type);

        <T> StampedReference<T> updateCached(Object type, StampedReference<T> value);
    }
    
    public static abstract class AbstractCachedNode<E extends AbstractCachedNode<E>> extends DefaultsNode.AbstractDefaultsNode<E> implements CachedNode<E> {

        protected long stamp;
        protected final Map<Object, StampedReference.Updater<?>> cache;

        protected AbstractCachedNode(
                NameTrie.Pointer<? extends E> parent) {
            super(pathOf(parent), parent, Maps.<ZNodeName, E>newHashMap());
            this.stamp = -1L;
            this.cache = Maps.newHashMap();
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
        @SuppressWarnings("unchecked")
        public <T> StampedReference<T> getCached(Object type) {
            StampedReference.Updater<?> updater = cache.get(type);
            if (updater != null) {
                return (StampedReference<T>) updater.get();
            } else {
                return null;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> StampedReference<T> updateCached(Object type, StampedReference<T> value) {
            touch(value.stamp());
            StampedReference.Updater<T> updater = (StampedReference.Updater<T>) cache.get(type);
            if (updater == null) {
                updater = StampedReference.Updater.newInstance(value);
                cache.put(type, updater);
                return null;
            } else {
                return updater.update(value);
            }
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper("")
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp)
                    .add("cache", cache.values()).toString();
        }
    }

    public static class SimpleCachedNode extends AbstractCachedNode<SimpleCachedNode> {

        public static SimpleCachedNode root() {
            return new SimpleCachedNode(SimpleNameTrie.<SimpleCachedNode>rootPointer());
        }

        protected SimpleCachedNode(
                NameTrie.Pointer<? extends SimpleCachedNode> parent) {
            super(parent);
        }
        
        @Override
        protected SimpleCachedNode newChild(ZNodeName label) {
            Pointer<SimpleCachedNode> pointer = weakPointer(label, this);
            return new SimpleCachedNode(pointer);
        }
    }

    // wrapper so that we can apply changes to the cache before our client sees them
    protected class PromiseWrapper extends PromiseTask<I,V> {

        protected PromiseWrapper(I task) {
            this(task, SettableFuturePromise.<V>create());
        }

        protected PromiseWrapper(I task, Promise<V> delegate) {
            super(task, delegate);
        }
        
        @Override
        public boolean set(V result) {
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
        protected Promise<V> delegate() {
            return delegate;
        }
    }
    
    protected final Logger logger;
    protected final ZxidTracker lastZxid;
    protected final ClientExecutor<? super I, V, SessionListener> client;
    protected final IConcurrentSet<CacheSessionListener<? super E>> listeners;
    
    protected ZNodeCacheTrie( 
            ClientExecutor<? super I, V, SessionListener> client,
            IConcurrentSet<CacheSessionListener<? super E>> listeners,
            E root) {
        super(root);
        this.logger = LogManager.getLogger(getClass());
        this.client = checkNotNull(client);
        this.listeners = checkNotNull(listeners);
        this.lastZxid = ZxidTracker.create();
    }
    
    public ZxidReference lastZxid() {
        return lastZxid;
    }
    
    public ClientExecutor<? super I, V, SessionListener> client() {
        return client;
    }
    
    @Override
    public void subscribe(CacheSessionListener<? super E> listener) {
        client.subscribe(listener);
        listeners.add(listener);
    }

    @Override
    public boolean unsubscribe(CacheSessionListener<? super E> listener) {
        boolean unsubscribed = client.unsubscribe(listener);
        unsubscribed = listeners.remove(listener) || unsubscribed;
        return unsubscribed;
    }

    @Override
    public ListenableFuture<V> submit(I request) {
        return client.submit(request, new PromiseWrapper(request));
    }
    
    @Override
    public ListenableFuture<V> submit(I request, Promise<V> promise) {
        return client.submit(request, new PromiseWrapper(request, promise));
    }

    public void handleCacheUpdate(CacheEvent<? extends E> event) {
        for (CacheSessionListener<? super E> listener: listeners) {
            listener.handleCacheUpdate(event);
        }
    }
    
    protected void handleResult(Records.Request request, Operation.ProtocolResponse<?> result) {
        long zxid = result.zxid();
        lastZxid.update(zxid);
        Records.Response response = result.record();
        
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
                    remove(path, zxid);
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
                add(path, zxid);    
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
                add(path, zxid);
                update(path, zxid, ImmutableList.of(request, response));
                break;
            }
            case DELETE:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                remove(path, zxid);
                break;
            }
            case CHECK:
            case EXISTS:
            case GET_ACL:
            case SET_ACL:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                add(path, zxid);
                update(path, zxid, ImmutableList.of(response));
                break;
            }
            case GET_CHILDREN:
            case GET_CHILDREN2:        
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                List<String> children = ((Records.ChildrenGetter) response).getChildren();
                E node = add(path, zxid);
                for (Map.Entry<ZNodeName, E> entry: node.entrySet()) {
                    if (! children.contains(entry.getKey().toString())) {
                        remove(entry.getValue().path(), zxid);
                    }
                }
                for (String child: children) {
                    add(path.join(ZNodeLabel.fromString(child)), zxid);
                }
                update(path, zxid, ImmutableList.of(response));
                break;
            }
            case GET_DATA:
            {
                ZNodePath path = (ZNodePath) ZNodeLabelVector.fromString(((Records.PathGetter) request).getPath());
                update(path, zxid, ImmutableList.of(response));
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
                update(path, zxid, ImmutableList.of(response));
                break;
            }
            default:
                break;
            }
        }
    }

    protected <T> void update(ZNodePath path, long stamp, Iterable<? extends Record> records) {
        ImmutableList.Builder<UpdateVisitor<?>> visitors = ImmutableList.builder();
        for (Record record: records) {
            if (record instanceof Records.StatGetter) {
                visitors.add(new UpdateVisitor<Records.StatGetter>(Records.StatGetter.class, StampedReference.of(stamp, (Records.StatGetter) record), StatEquivalence.STAT_EQUIVALENCE));
            }
            if (record instanceof Records.DataGetter) {
                visitors.add(new UpdateVisitor<Records.DataGetter>(Records.DataGetter.class, StampedReference.of(stamp, (Records.DataGetter) record), DataEquivalence.DATA_EQUIVALENCE));
            }
        }
        UpdatesVisitor visitor = new UpdatesVisitor(visitors.build());
        E node = add(path, stamp);
        visitor.apply(node);
    }

    protected E add(ZNodePath path, long stamp) {
        Iterator<ZNodeLabel> remaining = path.iterator();
        E node = root();
        while (remaining.hasNext()) {
            if (node.touch(stamp) < 0L) {
                handleCacheUpdate(NodeAddedCacheEvent.of(node));
            }
            node = node.putIfAbsent(remaining.next());
        }
        return node;
    }

    protected E remove(ZNodePath path, long stamp) {
        E node = get(path);
        if ((node != null) && (node.stamp() <= stamp)) {
            node.parent().get().remove(node.parent().name());
            node.touch(stamp);
            handleCacheUpdate(NodeRemovedCacheEvent.of(node));
            return node;
        } else {
            return null;
        }
    }
    
    public static interface Equivalence<T> {
        boolean equals(T a, T b);
    }

    public static enum DataEquivalence implements Equivalence<Records.DataGetter> {
        DATA_EQUIVALENCE;
        
        @Override
        public boolean equals(Records.DataGetter a, Records.DataGetter b) {
            return Arrays.equals(a.getData(), b.getData());
        }
    }

    public static enum StatEquivalence implements Equivalence<Records.StatGetter> {
        STAT_EQUIVALENCE;
        
        @Override
        public boolean equals(Records.StatGetter a, Records.StatGetter b) {
            return Objects.equal(a.getStat(), b.getStat());
        }
    }
    
    public class UpdateVisitor<T> implements Function<E, Boolean> {
    
        protected final Object type;
        protected final StampedReference<T> value;
        protected final Equivalence<? super T> equivalence;
        
        public UpdateVisitor(Object type, StampedReference<T> value, Equivalence<? super T> equivalence) {
            this.type = checkNotNull(type);
            this.value = checkNotNull(value);
            this.equivalence = checkNotNull(equivalence);
        }
        
        public Object getType() {
            return type;
        }
        
        public StampedReference<T> getValue() {
            return value;
        }
        
        public Equivalence<? super T> getEquivalence() {
            return equivalence;
        }
        
        @Override
        public Boolean apply(E node) {
            Boolean updated = Boolean.FALSE;
            if (node != null) {
                StampedReference<T> prev = node.updateCached(type, value);
                if (prev == null) {
                    updated = Boolean.TRUE;
                } else if (prev.stamp() < value.stamp()) {
                    updated = Boolean.valueOf(! equivalence.equals(prev.get(), value.get()));
                }
            }
            return updated;
        }
    }
    
    public class UpdatesVisitor implements Function<E, Optional<NodeUpdatedCacheEvent<E>>> {
        
        protected final ImmutableList<? extends UpdateVisitor<?>> visitors;
        
        public UpdatesVisitor(Iterable<? extends UpdateVisitor<?>> visitors) {
            this.visitors = ImmutableList.copyOf(visitors);
        }
        
        @Override
        public Optional<NodeUpdatedCacheEvent<E>> apply(E node) {
            ImmutableSet.Builder<Object> allTypes = ImmutableSet.builder();
            for (UpdateVisitor<?> visitor: visitors) {
                if (visitor.apply(node)) {
                    allTypes.add(visitor.getType());
                }
            }
            ImmutableSet<Object> types = allTypes.build();
            if (types.isEmpty()) {
                return Optional.absent();
            } else {
                NodeUpdatedCacheEvent<E> event = NodeUpdatedCacheEvent.of(node, types);
                handleCacheUpdate(event);
                return Optional.of(event);
            }
        }
    }
}
