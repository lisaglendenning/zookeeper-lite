package edu.uw.zookeeper.client;


import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.util.Event;
import edu.uw.zookeeper.data.StampedReference;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.data.ZNodeLabelTrie;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.data.ZNodeLabelTrie.SimplePointer;
import edu.uw.zookeeper.protocol.OpSessionResult;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.protocol.SessionRequestWrapper;
import edu.uw.zookeeper.protocol.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.Records.MultiOpResponse;
import edu.uw.zookeeper.protocol.client.ZxidTracker;
import edu.uw.zookeeper.protocol.proto.IGetACLResponse;
import edu.uw.zookeeper.protocol.proto.IGetDataResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;

/**
 * Only caches the results of operations submitted through this wrapper.
 */
public class ZNodeResponseCache<E extends ZNodeResponseCache.AbstractNodeCache<E>> implements ClientExecutor {

    public static ZNodeResponseCache<SimpleZNodeCache> newInstance(
            Publisher publisher, ClientExecutor client) {
        return newInstance(publisher, client, SimpleZNodeCache.root());
    }
    
    public static <E extends ZNodeResponseCache.AbstractNodeCache<E>> ZNodeResponseCache<E> newInstance(
            Publisher publisher, ClientExecutor client, E root) {
        return newInstance(publisher, client, ZNodeLabelTrie.of(root));
    }
    
    public static <E extends ZNodeResponseCache.AbstractNodeCache<E>> ZNodeResponseCache<E> newInstance(
            Publisher publisher, ClientExecutor client, ZNodeLabelTrie<E> trie) {
        return new ZNodeResponseCache<E>(publisher, client, trie);
    }

    public static enum View {
        DATA(Records.DataHolder.class), 
        ACL(Records.AclHolder.class), 
        STAT(Records.StatHolder.class);
        
        public static View ofType(Class<? extends Records.View> type) {
            for (View v: values()) {
                if (type == v.type()) {
                    return v;
                }
            }
            throw new IllegalArgumentException(type.toString());
        }
        
        private final Class<? extends Records.View> type;
        
        private View(Class<? extends Records.View> type) {
            this.type = type;
        }
        
        public Class<? extends Records.View> type() {
            return type;
        }
    }
    
    @Event
    public static class ViewUpdate {
        
        public static ViewUpdate ifUpdated(
                View view, 
                StampedReference<? extends Records.View> previousValue, 
                StampedReference<? extends Records.View> updatedValue) {
            
            if (updatedValue.stamp().compareTo(previousValue.stamp()) < 0) {
                return null;
            }
            
            switch (view) {
            case DATA:
            {
                byte[] prev = ((Records.DataHolder)previousValue.get()).getData();
                byte[] updated = ((Records.DataHolder)updatedValue.get()).getData();
                if (Arrays.equals(prev, updated)) {
                    return null;
                }
                break;
            }
            case ACL:
            {
                List<ACL> prev = ((Records.AclHolder)previousValue.get()).getAcl();
                List<ACL> updated = ((Records.AclHolder)updatedValue.get()).getAcl();
                if (prev.equals(updated)) {
                    return null;
                }
                break;
            }
            case STAT:
            {
                Stat prev = ((Records.StatHolder)previousValue.get()).getStat();
                Stat updated = ((Records.StatHolder)updatedValue.get()).getStat();
                if (prev.equals(updated)) {
                    return null;
                }
                break;
            }
            default:
                throw new AssertionError();
            }
            
            return of(view, previousValue, updatedValue);
        }
        
        public static ViewUpdate of(
                View view, 
                StampedReference<? extends Records.View> previousValue, 
                StampedReference<? extends Records.View> updatedValue) {
            return new ViewUpdate(view, previousValue, updatedValue);
        }
        
        private final View view;
        private final StampedReference<? extends Records.View> previousValue;
        private final StampedReference<? extends Records.View> updatedValue;
        
        public ViewUpdate(
                View view, 
                StampedReference<? extends Records.View> previousValue, 
                StampedReference<? extends Records.View> updatedValue) {
            this.view = view;
            this.previousValue = previousValue;
            this.updatedValue = updatedValue;
        }
        
        public View view() {
            return view;
        }
        
        public StampedReference<? extends Records.View> previousValue() {
            return previousValue;
        }
        
        public StampedReference<? extends Records.View> updatedValue() {
            return updatedValue;
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
        
        <T extends Records.View> StampedReference<T> asView(View view);

        <T extends Records.View> StampedReference<T> update(View view, StampedReference<T> value);
    }
    
    public abstract static class AbstractNodeCache<E extends AbstractNodeCache<E>> extends ZNodeLabelTrie.DefaultsNode<E> implements NodeCache<E> {

        protected final AtomicLong stamp;
        protected final Map<View, StampedReference.Updater<? extends Records.View>> views;

        protected AbstractNodeCache(
                Optional<ZNodeLabelTrie.Pointer<E>> parent) {
            this(parent,
                    Collections.synchronizedMap(Maps.<View, StampedReference.Updater<? extends Records.View>>newEnumMap(View.class)));
        }
        
        protected AbstractNodeCache(
                Optional<ZNodeLabelTrie.Pointer<E>> parent,
                Map<View, StampedReference.Updater<? extends Records.View>> views) {
            super(parent);
            this.stamp = new AtomicLong(0L);
            this.views = views;
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
        public <T extends Records.View> StampedReference<T> asView(View view) {
            StampedReference.Updater<? extends Records.View> updater = views.get(view);
            if (updater != null) {
                return (StampedReference<T>) updater.get();
            } else {
                return null;
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends Records.View> StampedReference<T> update(View view, StampedReference<T> value) {
            StampedReference.Updater<T> updater = (StampedReference.Updater<T>) views.get(view);
            if (updater == null) {
                synchronized (views) {
                    if (views.containsKey(view)) {
                        updater = (StampedReference.Updater<T>) views.get(view);
                    } else {
                        updater = StampedReference.Updater.<T>newInstance(value);
                        views.put(view, updater);
                    }
                }
            }
            touch(value.stamp());
            return updater.setIfGreater(value);
        }
        
        @Override
        public String toString() {
            Map<View, String> viewStr = Maps.newHashMap();
            for (Map.Entry<View, StampedReference.Updater<? extends Records.View>> entry: views.entrySet()) {
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
    protected class PromiseWrapper extends ForwardingPromise<Operation.SessionResult> {

        protected final Promise<Operation.SessionResult> delegate;
        
        protected PromiseWrapper() {
            this(SettableFuturePromise.<Operation.SessionResult>create());
        }

        protected PromiseWrapper(Promise<Operation.SessionResult> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public boolean set(Operation.SessionResult result) {
            if (! isDone()) {
                handleResult(result);
            }
            return super.set(result);
        }
        
        @Override
        protected Promise<Operation.SessionResult> delegate() {
            return delegate;
        }
    }
    
    protected final ZxidTracker lastZxid;
    protected final ZNodeLabelTrie<E> trie;
    protected final ClientExecutor client;
    protected final Publisher publisher;
    
    protected ZNodeResponseCache(
            Publisher publisher, ClientExecutor client, ZNodeLabelTrie<E> trie) {
        this.trie = trie;
        this.client = client;
        this.lastZxid = ZxidTracker.create();
        this.publisher = publisher;
    }
    
    public ClientExecutor client() {
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
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request) {
        return client.submit(request, new PromiseWrapper());
    }
    
    @Override
    public ListenableFuture<Operation.SessionResult> submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        return client.submit(request, new PromiseWrapper(promise));
    }
    
    public E get(ZNodeLabel.Path path) {
        return trie().get(path);
    }
    
    public void handleResult(Operation.SessionResult result) {
        Long zxid = result.reply().zxid();
        lastZxid.update(zxid);
        Operation.Reply reply = result.reply().reply();
        Operation.Request request = result.request().request();
        
        if (reply instanceof Operation.Error 
                && (KeeperException.Code.NONODE == ((Operation.Error)reply).error())) {
            ZNodeLabel.Path path = ZNodeLabel.Path.of(((Records.PathHolder) ((Operation.RecordHolder<?>)request).asRecord()).getPath());
            switch (request.opcode()) {
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
        
        switch (request.opcode()) {
        case CHECK:
            // TODO
            break;
        case CREATE:
        case CREATE2:
        {
            if (reply instanceof Operation.Response) {
                Records.PathHolder responseRecord = (Records.PathHolder) ((Operation.RecordHolder<?>)reply).asRecord();
                E node = add(ZNodeLabel.Path.of(responseRecord.getPath()), zxid);
                Records.CreateRecord record = (Records.CreateRecord)((Operation.RecordHolder<?>)request).asRecord();
                StampedReference<Records.CreateRecord> stampedRequest = StampedReference.of(zxid, record);
                update(node, stampedRequest);
                if (responseRecord instanceof Records.StatRecord) {
                    StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                    update(node, stampedResponse);
                }
            } else if (KeeperException.Code.NODEEXISTS == ((Operation.Error)reply).error()) {
                Records.PathHolder requestRecord = (Records.PathHolder) ((Operation.RecordHolder<?>)request).asRecord();
                add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
            }
            break;
        }
        case DELETE:
        {
            if (reply instanceof Operation.Response 
                    || (KeeperException.Code.NONODE == ((Operation.Error)reply).error())) {
                Records.PathHolder requestRecord = (Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord();
                ZNodeLabel.Path path = ZNodeLabel.Path.of(requestRecord.getPath());
                remove(path, zxid);
                break;
            }
        }
        case EXISTS:
        {
            Records.PathHolder requestRecord = (Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)((Operation.RecordHolder<?>)reply).asRecord());
                update(node, stampedResponse);
            }
            break;
        }
        case GET_ACL:
        {
            Records.PathHolder requestRecord = (Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                StampedReference<IGetACLResponse> stampedResponse = StampedReference.of(
                        result.reply().zxid(), (IGetACLResponse)((Operation.RecordHolder<?>)reply).asRecord());
                update(node, stampedResponse);
            }
            break;
        }
        case GET_CHILDREN:
        case GET_CHILDREN2:        
        {
            Records.PathHolder requestRecord = (Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                Records.ChildrenHolder responseRecord = (Records.ChildrenHolder) ((Operation.RecordHolder<?>)reply).asRecord();
                List<String> children = responseRecord.getChildren();
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
                if (responseRecord instanceof Records.StatRecord) {
                    StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                    update(node, stampedResponse);
                }
            }
            break;
        }
        case GET_DATA:
        {
            Records.PathHolder requestRecord = (Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                StampedReference<IGetDataResponse> stampedResponse = StampedReference.of(
                        result.reply().zxid(), (IGetDataResponse)((Operation.RecordHolder<?>)reply).asRecord());
                update(node, stampedResponse);
            }
            break;
        }
        case MULTI:
        {
            if (reply instanceof Operation.Response) {
                int xid = result.request().xid();
                IMultiRequest requestRecord = (IMultiRequest) ((Operation.RecordHolder<?>)request).asRecord();
                IMultiResponse responseRecord = (IMultiResponse) ((Operation.RecordHolder<?>)reply).asRecord();
                Iterator<MultiOpRequest> requests = requestRecord.iterator();
                Iterator<MultiOpResponse> responses = responseRecord.iterator();
                while (requests.hasNext()) {
                    checkArgument(responses.hasNext());
                    Operation.SessionResult nestedResult = OpSessionResult.of(
                            SessionRequestWrapper.newInstance(xid, requests.next()), 
                            SessionReplyWrapper.create(xid, zxid, responses.next()));
                    handleResult(nestedResult);
                }
            } else {
                // TODO
            }
            break;
        }
        case SET_ACL:
        {
            ISetACLRequest requestRecord = (ISetACLRequest) ((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                update(node, StampedReference.of(zxid, requestRecord));
                Records.StatHolder responseRecord = (Records.StatHolder) ((Operation.RecordHolder<?>)reply).asRecord();
                update(node, StampedReference.of(zxid, responseRecord));
            }
            break;
        }
        case SET_DATA:
        {
            ISetDataRequest requestRecord = (ISetDataRequest) ((Operation.RecordHolder<?>)request).asRecord();
            if (reply instanceof Operation.Response) {
                E node = add(ZNodeLabel.Path.of(requestRecord.getPath()), zxid);
                update(node, StampedReference.of(zxid, requestRecord));
                Records.StatHolder responseRecord = (Records.StatHolder) ((Operation.RecordHolder<?>)reply).asRecord();
                update(node, StampedReference.of(zxid, responseRecord));
            }
            break;
        }
        default:
            break;
        }
    }
    
    protected boolean update(E node, StampedReference<? extends Records.View> value) {
        boolean changed = false;
        for (View view: View.values()) {
            if (view.type().isInstance(value.get())) {
                StampedReference<? extends Records.View> prev = node.update(view, value);
                ViewUpdate event = ViewUpdate.ifUpdated(view, prev, value);
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
                if (next.touch(zxid).longValue() == 0L) {
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
