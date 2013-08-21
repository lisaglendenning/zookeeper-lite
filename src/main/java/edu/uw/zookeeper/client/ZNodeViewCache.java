package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.common.Event;
import edu.uw.zookeeper.common.Promise;
import edu.uw.zookeeper.common.PromiseTask;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.SettableFuturePromise;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.StampedReference.Updater;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.data.ZNodeLabelTrie.SimplePointer;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.proto.IGetACLResponse;
import edu.uw.zookeeper.protocol.proto.IGetDataResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpResponse;
import edu.uw.zookeeper.protocol.server.ZxidReference;

/**
 * Only caches the results of operations submitted through this wrapper.
 */
public class ZNodeViewCache<E extends ZNodeViewCache.AbstractNodeCache<E>, I extends Operation.Request, V extends Operation.ProtocolResponse<?>> 
        implements ClientExecutor<I, V> {

    public static <I extends Operation.Request, V extends Operation.ProtocolResponse<?>> ZNodeViewCache<SimpleZNodeCache,I,V> newInstance(
            Publisher publisher, ClientExecutor<I,V> client) {
        return newInstance(publisher, client, SimpleZNodeCache.root());
    }
    
    public static <E extends ZNodeViewCache.AbstractNodeCache<E>, I extends Operation.Request, V extends Operation.ProtocolResponse<?>> ZNodeViewCache<E,I,V> newInstance(
            Publisher publisher, ClientExecutor<I,V> client, E root) {
        return newInstance(publisher, client, ZNodeLabelTrie.of(root));
    }
    
    public static <E extends ZNodeViewCache.AbstractNodeCache<E>, I extends Operation.Request, V extends Operation.ProtocolResponse<?>> ZNodeViewCache<E,I,V> newInstance(
            Publisher publisher, ClientExecutor<I,V> client, ZNodeLabelTrie<E> trie) {
        return new ZNodeViewCache<E,I,V>(publisher, client, trie);
    }

    public static enum View {
        DATA(Records.DataGetter.class), 
        ACL(Records.AclGetter.class), 
        STAT(Records.StatGetter.class);
        
        public static View ofType(Class<? extends Records.ZNodeView> type) {
            for (View v: values()) {
                if (type == v.type()) {
                    return v;
                }
            }
            throw new IllegalArgumentException(type.toString());
        }
        
        private final Class<? extends Records.ZNodeView> type;
        
        private View(Class<? extends Records.ZNodeView> type) {
            this.type = type;
        }
        
        public Class<? extends Records.ZNodeView> type() {
            return type;
        }
    }
    
    @Event
    public static class ViewUpdate {
        
        public static ViewUpdate ifUpdated(
                ZNodeLabel.Path path,
                View view, 
                StampedReference<? extends Records.ZNodeView> previous, 
                StampedReference<? extends Records.ZNodeView> updated) {
            
            if (updated.stamp().compareTo(previous.stamp()) < 0) {
                return null;
            }
            
            if (previous.get() != null) {
                switch (view) {
                case DATA:
                {
                    if (Arrays.equals(((Records.DataGetter)previous.get()).getData(), 
                            ((Records.DataGetter)updated.get()).getData())) {
                        return null;
                    }
                    break;
                }
                case ACL:
                {
                    if (Objects.equal(((Records.AclGetter)previous.get()).getAcl(), 
                            ((Records.AclGetter)updated.get()).getAcl())) {
                        return null;
                    }
                    break;
                }
                case STAT:
                {
                    if (Objects.equal(((Records.StatGetter)previous.get()).getStat(),
                            ((Records.StatGetter)updated.get()).getStat())) {
                        return null;
                    }
                    break;
                }
                default:
                    throw new AssertionError();
                }
            }
            
            return of(path, view, previous, updated);
        }
        
        public static ViewUpdate of(
                ZNodeLabel.Path path,
                View view, 
                StampedReference<? extends Records.ZNodeView> previous, 
                StampedReference<? extends Records.ZNodeView> updated) {
            return new ViewUpdate(path, view, previous, updated);
        }
        
        private final ZNodeLabel.Path path;
        private final View view;
        private final StampedReference<? extends Records.ZNodeView> previous;
        private final StampedReference<? extends Records.ZNodeView> updated;
        
        public ViewUpdate(
                ZNodeLabel.Path path,
                View view, 
                StampedReference<? extends Records.ZNodeView> previous, 
                StampedReference<? extends Records.ZNodeView> updated) {
            this.path = path;
            this.view = view;
            this.previous = previous;
            this.updated = updated;
        }
        
        public ZNodeLabel.Path path() {
            return path;
        }
        
        public View view() {
            return view;
        }
        
        public StampedReference<? extends Records.ZNodeView> previous() {
            return previous;
        }
        
        public StampedReference<? extends Records.ZNodeView> updated() {
            return updated;
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("view", view())
                    .add("previous", previous())
                    .add("updated", updated()).toString();
        }
    }
    
    @Event
    public static class NodeUpdate extends AbstractPair<NodeUpdate.UpdateType, StampedReference<ZNodeLabel.Path>> {

        public static NodeUpdate of(UpdateType type, StampedReference<ZNodeLabel.Path> path) {
            return new NodeUpdate(type, path);
        }
        
        public static enum UpdateType {
            NODE_ADDED, NODE_REMOVED;
        }
        
        public NodeUpdate(UpdateType type, StampedReference<ZNodeLabel.Path> path) {
            super(type, path);
        }        
        
        public UpdateType type() {
            return first;
        }
        
        public StampedReference<ZNodeLabel.Path> path() {
            return second;
        }
    }
    
    public static interface NodeCache<E extends NodeCache<E>> extends ZNodeLabelTrie.Node<E> {

        Long stamp();
        
        Long touch(Long zxid);
        
        <T extends Records.ZNodeView> StampedReference<T> asView(View view);

        <T extends Records.ZNodeView> StampedReference<T> update(View view, StampedReference<T> value);
    }
    
    public abstract static class AbstractNodeCache<E extends AbstractNodeCache<E>> extends ZNodeLabelTrie.DefaultsNode<E> implements NodeCache<E> {

        protected final AtomicLong stamp;
        protected final ConcurrentMap<View, StampedReference.Updater<? extends Records.ZNodeView>> views;

        protected AbstractNodeCache(
                Optional<ZNodeLabelTrie.Pointer<E>> parent) {
            super(parent);
            this.stamp = new AtomicLong(0L);
            this.views = new ConcurrentHashMap<View, StampedReference.Updater<? extends Records.ZNodeView>>();
        }
        
        @Override
        public Long stamp() {
            return stamp.get();
        }
        
        @Override
        public Long touch(Long zxid) {
            Long current = stamp.get();
            if (current.compareTo(zxid) < 0) {
                if (! stamp.compareAndSet(current, zxid)) {
                    // try again
                    return touch(zxid);
                }
            }
            return current;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Records.ZNodeView> StampedReference<T> asView(View view) {
            StampedReference.Updater<? extends Records.ZNodeView> updater = views.get(view);
            if (updater != null) {
                return (StampedReference<T>) updater.get();
            } else {
                return null;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Records.ZNodeView> StampedReference<T> update(View view, StampedReference<T> value) {
            touch(value.stamp());
            StampedReference.Updater<T> updater = (StampedReference.Updater<T>) views.get(view);
            if (updater == null) {
                updater = StampedReference.Updater.<T>newInstance(StampedReference.<T>of(0L, null));
                StampedReference.Updater<T> prev = (Updater<T>) views.putIfAbsent(view, updater);
                if (prev != null) {
                    updater = prev;
                }
            }
            return updater.setIfGreater(value);
        }
        
        @Override
        public String toString() {
            Map<View, String> viewStr = Maps.newHashMap();
            for (Map.Entry<View, StampedReference.Updater<? extends Records.ZNodeView>> entry: views.entrySet()) {
                viewStr.put(
                        entry.getKey(), 
                        String.format("(%s, %s)", 
                                entry.getValue().get().stamp(), 
                                Records.toString((Record) entry.getValue().get().get())));
            }
            return Objects.toStringHelper(this)
                    .add("path", path())
                    .add("children", keySet())
                    .add("stamp", stamp())
                    .add("views", viewStr).toString();
        }
    }
    
    public static class SimpleZNodeCache extends AbstractNodeCache<SimpleZNodeCache> {

        public static SimpleZNodeCache root() {
            return new SimpleZNodeCache(Optional.<Pointer<SimpleZNodeCache>>absent());
        }

        protected SimpleZNodeCache(
                Optional<Pointer<SimpleZNodeCache>> parent) {
            super(parent);
        }

        @Override
        protected SimpleZNodeCache newChild(ZNodeLabel.Component label) {
            Pointer<SimpleZNodeCache> pointer = SimplePointer.of(label, this);
            return new SimpleZNodeCache(Optional.of(pointer));
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
                                    ((Operation.RecordHolder<?>) task()).getRecord());
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
    protected final ZNodeLabelTrie<E> trie;
    protected final ClientExecutor<? super I, V> client;
    protected final Publisher publisher;
    
    protected ZNodeViewCache(
            Publisher publisher, ClientExecutor<? super I, V> client, ZNodeLabelTrie<E> trie) {
        this.logger = LogManager.getLogger(getClass());
        this.trie = trie;
        this.client = client;
        this.lastZxid = ZxidTracker.create();
        this.publisher = publisher;
    }
    
    public ZxidReference lastZxid() {
        return lastZxid;
    }
    
    public ClientExecutor<? super I, V> client() {
        return client;
    }
    
    public ZNodeLabelTrie<E> trie() {
        return trie;
    }

    @Override
    public void register(Object object) {
        client().register(object);
        publisher.register(object);
    }

    @Override
    public void unregister(Object object) {
        client().unregister(object);
        try {
            publisher.unregister(object);
        } catch (IllegalArgumentException e) {}
    }

    @Override
    public ListenableFuture<V> submit(I request) {
        return client.submit(request, new PromiseWrapper(request));
    }
    
    @Override
    public ListenableFuture<V> submit(I request, Promise<V> promise) {
        return client.submit(request, new PromiseWrapper(request, promise));
    }

    public boolean contains(ZNodeLabel.Path path) {
        return get(path) != null;
    }
    
    public E get(ZNodeLabel.Path path) {
        return trie().get(path);
    }
    
    protected void handleResult(Records.Request request, Operation.ProtocolResponse<?> result) {
        Long zxid = result.getZxid();
        lastZxid.update(zxid);
        Records.Response response = result.getRecord();
        
        if (response instanceof Operation.Error 
                && (KeeperException.Code.NONODE == ((Operation.Error)response).getError())) {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathGetter) request).getPath());
            switch (request.getOpcode()) {
            case CREATE:
            case CREATE2:
                path = (ZNodeLabel.Path) path.head();
            case CHECK:
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
        }
        
        switch (request.getOpcode()) {
        case CHECK:
            // TODO
            break;
        case CREATE:
        case CREATE2:
        {
            if (! (response instanceof Operation.Error)) {
                E node = add(ZNodeLabel.Path.of(((Records.PathGetter) response).getPath()), zxid);
                StampedReference<Records.CreateModeGetter> stampedRequest = StampedReference.of(zxid, (Records.CreateModeGetter) request);
                update(node, stampedRequest);
                if (response instanceof Records.StatGetter) {
                    StampedReference<Records.StatGetter> stampedResponse = StampedReference.of(zxid, (Records.StatGetter) response);
                    update(node, stampedResponse);
                }
            } else if (KeeperException.Code.NODEEXISTS == ((Operation.Error)response).getError()) {
                add(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
            }
            break;
        }
        case DELETE:
        {
            if (! (response instanceof Operation.Error) 
                    || (KeeperException.Code.NONODE == ((Operation.Error)response).getError())) {
                remove(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
                break;
            }
        }
        case EXISTS:
        {
            if (! (response instanceof Operation.Error)) {
                E node = add(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
                StampedReference<Records.StatGetter> stampedResponse = StampedReference.of(zxid, (Records.StatGetter)response);
                update(node, stampedResponse);
            }
            break;
        }
        case GET_ACL:
        {
            if (! (response instanceof Operation.Error)) {
                E node = add(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
                StampedReference<IGetACLResponse> stampedResponse = StampedReference.of(
                        zxid, (IGetACLResponse)response);
                update(node, stampedResponse);
            }
            break;
        }
        case GET_CHILDREN:
        case GET_CHILDREN2:        
        {
            if (! (response instanceof Operation.Error)) {
                E node = add(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
                List<String> children = ((Records.ChildrenGetter) response).getChildren();
                for (String component: children) {
                    E child = node.add(component);
                    if (child.touch(zxid).longValue() == 0L) {
                        post(NodeUpdate.of(
                                NodeUpdate.UpdateType.NODE_ADDED, 
                                StampedReference.of(zxid, child.path())));
                    }
                }
                for (Map.Entry<ZNodeLabel.Component, E> entry: node.entrySet()) {
                    if (! children.contains(entry.getKey().toString())) {
                        remove(entry.getValue().path(), zxid);
                    }
                }
                if (response instanceof Records.StatGetter) {
                    StampedReference<Records.StatGetter> stampedResponse = StampedReference.of(zxid, (Records.StatGetter)response);
                    update(node, stampedResponse);
                }
            }
            break;
        }
        case GET_DATA:
        {
            if (! (response instanceof Operation.Error)) {
                E node = add(ZNodeLabel.Path.of(((Records.PathGetter) request).getPath()), zxid);
                StampedReference<IGetDataResponse> stampedResponse = StampedReference.of(
                        zxid, (IGetDataResponse) response);
                update(node, stampedResponse);
            }
            break;
        }
        case MULTI:
        {
            if (! (response instanceof Operation.Error)) {
                int xid = result.getXid();
                IMultiRequest requestRecord = (IMultiRequest) request;
                IMultiResponse responseRecord = (IMultiResponse) response;
                Iterator<MultiOpRequest> requests = requestRecord.iterator();
                Iterator<MultiOpResponse> responses = responseRecord.iterator();
                while (requests.hasNext()) {
                    checkArgument(responses.hasNext());
                     handleResult(requests.next(),
                            ProtocolResponseMessage.of(xid, zxid, responses.next()));
                }
            } else {
                // TODO ?
            }
            break;
        }
        case SET_ACL:
        {
            if (! (response instanceof Operation.Error)) {
                ISetACLRequest requestRecord = (ISetACLRequest) request;
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                update(node, StampedReference.of(zxid, requestRecord));
                Records.StatGetter responseRecord = (Records.StatGetter) response;
                update(node, StampedReference.of(zxid, responseRecord));
            }
            break;
        }
        case SET_DATA:
        {
            if (! (response instanceof Operation.Error)) {
                ISetDataRequest requestRecord = (ISetDataRequest) request;
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                update(node, StampedReference.of(zxid, requestRecord));
                Records.StatGetter responseRecord = (Records.StatGetter) response;
                update(node, StampedReference.of(zxid, responseRecord));
            }
            break;
        }
        default:
            break;
        }
    }
    
    protected boolean update(E node, StampedReference<? extends Records.ZNodeView> value) {
        boolean changed = false;
        for (View view: View.values()) {
            if (view.type().isInstance(value.get())) {
                StampedReference<? extends Records.ZNodeView> prev = node.update(view, value);
                ViewUpdate event = ViewUpdate.ifUpdated(node.path(), view, prev, value);
                if (event != null) {
                    changed = true;
                    post(event);
                }
            }
        }
        return changed;
    }

    protected E add(ZNodeLabel.Path path, Long zxid) {
        E parent = trie().root();
        E next = parent;
        parent.touch(zxid);
        for (ZNodeLabel.Component e: path) {
            next = parent.get(e);
            if (next == null) {
                next = parent.add(e);
                if (next.touch(zxid).equals(Long.valueOf(0L))) {
                    post(NodeUpdate.of(
                            NodeUpdate.UpdateType.NODE_ADDED, 
                            StampedReference.of(zxid, next.path())));
                }
            } else {
                next.touch(zxid);
            }
            parent = next;
        }
        return next;
    }

    protected E remove(ZNodeLabel.Path path, Long zxid) {
        E node = get(path);
        if (node != null && node.stamp().compareTo(zxid) <= 0) {
            node = trie().remove(path);
            if (node != null) {
                post(NodeUpdate.of(
                        NodeUpdate.UpdateType.NODE_REMOVED, 
                        StampedReference.of(zxid, path)));
            }
        } else {
            node = null;
        }
        return node;
    }

    protected void post(Object object) {
        publisher.post(object);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(trie().toString()).toString();
    }
}
