package edu.uw.zookeeper.data;


import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.jute.Record;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.Event;
import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.data.ZNodeLabelTrie.Pointer;
import edu.uw.zookeeper.data.ZNodeLabelTrie.SimplePointer;
import edu.uw.zookeeper.protocol.OpSessionResult;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.SessionReplyWrapper;
import edu.uw.zookeeper.protocol.SessionRequestWrapper;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection.RequestFuture;
import edu.uw.zookeeper.protocol.proto.IGetACLResponse;
import edu.uw.zookeeper.protocol.proto.IGetDataResponse;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.ChildrenRecord;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpRequest;
import edu.uw.zookeeper.protocol.proto.Records.MultiOpResponse;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;

/**
 * Only caches the results of operations submitted through this wrapper.
 */
public class ZNodeResponseCacheTrie<E extends ZNodeResponseCacheTrie.ZNodeCache<E>> extends ForwardingEventful implements ClientExecutor {

    public static <E extends ZNodeResponseCacheTrie.ZNodeCache<E>> ZNodeResponseCacheTrie<E> newInstance(
            Publisher publisher, ClientExecutor client, E root) {
        return new ZNodeResponseCacheTrie<E>(publisher, client, root);
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
                if (! Arrays.equals(prev, updated)) {
                    return null;
                }
                break;
            }
            case ACL:
            {
                List<ACL> prev = ((Records.AclHolder)previousValue.get()).getAcl();
                List<ACL> updated = ((Records.AclHolder)updatedValue.get()).getAcl();
                if (! prev.equals(updated)) {
                    return null;
                }
                break;
            }
            case STAT:
            {
                Stat prev = ((Records.StatHolder)previousValue.get()).getStat();
                Stat updated = ((Records.StatHolder)updatedValue.get()).getStat();
                if (! prev.equals(updated)) {
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
    
    public abstract static class ZNodeCache<E extends ZNodeCache<E>> extends ZNodeLabelTrie.AbstractNode<E> {

        protected final Map<View, StampedReference.Updater<? extends Records.View>> views;
        
        protected ZNodeCache(
                Optional<ZNodeLabelTrie.Pointer<E>> parent) {
            super(parent);
            this.views = Collections.synchronizedMap(Maps.<View, StampedReference.Updater<? extends Records.View>>newEnumMap(View.class));
        }

        @SuppressWarnings("unchecked")
        public <T extends Records.View> StampedReference<? extends T> asView(View view) {
            StampedReference.Updater<? extends Records.View> updater = views.get(view);
            if (updater != null) {
                return (StampedReference<? extends T>) updater.get();
            } else {
                return null;
            }
        }
        
        @SuppressWarnings("unchecked")
        public <T extends Records.View> StampedReference<? extends T> update(View view, StampedReference<T> value) {
            StampedReference.Updater<T> updater = (StampedReference.Updater<T>) views.get(view);
            if (updater == null) {
                synchronized (views) {
                    if (! views.containsKey(view)) {
                        updater = StampedReference.Updater.<T>newInstance(value);
                        StampedReference.Updater<? extends Records.View> prev = (StampedReference.Updater<T>) views.put(view, updater);
                        assert (prev == null);
                    } else {
                        updater = (StampedReference.Updater<T>) views.get(view);
                    }
                }
            }
            return updater.setIfGreater(value);
        }
        
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("path", path()).add("views", views).toString();
        }
    }
    
    public static class SimpleZNodeCache extends ZNodeCache<SimpleZNodeCache> {

        public static SimpleZNodeCache root() {
            return new SimpleZNodeCache(Optional.<Pointer<SimpleZNodeCache>>absent());
        }

        protected SimpleZNodeCache(
                Optional<Pointer<SimpleZNodeCache>> parent) {
            super(parent);
        }

        @Override
        protected SimpleZNodeCache newChild(ZNodeLabel.Component label) {
            Pointer<SimpleZNodeCache> childPointer = SimplePointer.of(label, this);
            return new SimpleZNodeCache(Optional.of(childPointer));
        }
    }
    
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
    
    protected final ZNodeLabelTrie<E> trie;
    protected final ClientExecutor client;
    
    protected ZNodeResponseCacheTrie(
            Publisher publisher, ClientExecutor client, E root) {
        super(publisher);
        this.trie = ZNodeLabelTrie.of(root);
        this.client = client;
        client.register(this);
    }
    
    public ClientExecutor asClient() {
        return client;
    }
    
    public ZNodeLabelTrie<E> asTrie() {
        return trie;
    }

    @Override
    public void register(Object object) {
        asClient().register(object);
    }

    @Override
    public void unregister(Object object) {
        asClient().unregister(object);
    }

    @Override
    public ClientProtocolConnection.RequestFuture submit(Operation.Request request) {
        // wrapper so that we can apply changes before our client sees them
        return client.submit(request, new PromiseWrapper());
    }
    
    @Override
    public RequestFuture submit(Operation.Request request, Promise<Operation.SessionResult> promise) {
        // wrapper so that we can apply changes before our client sees them
        return client.submit(request, new PromiseWrapper(promise));
        
    }

    protected boolean handleResult(Operation.SessionResult result) {
        if (result.reply().reply() instanceof Operation.Error) {
            // no updates to apply
            return false;
        }
        Operation.Response response = (Operation.Response) (result.reply().reply());
        Operation.Request request = result.request().request();
        
        boolean changed = true;
        switch (request.opcode()) {
        case CREATE:
        case CREATE2:
        {
            Record responseRecord = ((Operation.RecordHolder<?>)response).asRecord();
            E node = asTrie().put(((Records.PathHolder)responseRecord).getPath());
            Long zxid = result.reply().zxid();
            Records.CreateRecord record = (Records.CreateRecord)((Operation.RecordHolder<?>)request).asRecord();
            StampedReference<Records.CreateRecord> stampedRequest = StampedReference.of(zxid, record);
            update(node, stampedRequest);
            if (responseRecord instanceof Records.StatRecord) {
                StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                update(node, stampedResponse);
            }
            break;
        }
        case DELETE:
        {
            Records.PathRecord record = (Records.PathRecord)((Operation.RecordHolder<?>)request).asRecord();
            asTrie().remove(record.getPath());
            break;
        }
        case EXISTS:
        {
            E node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            Long zxid = result.reply().zxid();
            StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)((Operation.RecordHolder<?>)response).asRecord());
            update(node, stampedResponse);
            break;
        }
        case GET_ACL:
        {
            E node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            StampedReference<IGetACLResponse> stampedResponse = StampedReference.of(
                    result.reply().zxid(), (IGetACLResponse)((Operation.RecordHolder<?>)response).asRecord());
            update(node, stampedResponse);
            break;
        }
        case GET_CHILDREN:
        case GET_CHILDREN2:        
        {
            E node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            Records.ChildrenRecord responseRecord = (ChildrenRecord) ((Operation.RecordHolder<?>)response).asRecord();
            for (String child: responseRecord.getChildren()) {
                node.put(child);
            }
            if (responseRecord instanceof Records.StatRecord) {
                Long zxid = result.reply().zxid();
                StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                update(node, stampedResponse);
            }
            break;
        }
        case GET_DATA:
        {
            E node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            StampedReference<IGetDataResponse> stampedResponse = StampedReference.of(
                    result.reply().zxid(), (IGetDataResponse)((Operation.RecordHolder<?>)response).asRecord());
            update(node, stampedResponse);
            break;
        }
        case MULTI:
        {
            int xid = result.request().xid();
            long zxid = result.reply().zxid();
            IMultiRequest requestRecord = (IMultiRequest) ((Operation.RecordHolder<?>)request).asRecord();
            IMultiResponse responseRecord = (IMultiResponse) ((Operation.RecordHolder<?>)response).asRecord();
            Iterator<MultiOpRequest> requests = requestRecord.iterator();
            Iterator<MultiOpResponse> responses = responseRecord.iterator();
            while (requests.hasNext()) {
                checkArgument(responses.hasNext());
                Operation.SessionResult nestedResult = OpSessionResult.of(
                        SessionRequestWrapper.newInstance(xid, requests.next()), 
                        SessionReplyWrapper.create(xid, zxid, responses.next()));
                handleResult(nestedResult);
            }
            break;
        }
        case SET_ACL:
        {
            ISetACLRequest requestRecord = (ISetACLRequest) ((Operation.RecordHolder<?>)request).asRecord();
            E node = asTrie().get(requestRecord.getPath());
            Long zxid = result.reply().zxid();
            update(node, StampedReference.of(zxid, requestRecord));
            Records.StatRecord responseRecord = (Records.StatRecord) ((Operation.RecordHolder<?>)response).asRecord();
            update(node, StampedReference.of(zxid, responseRecord));
            break;
        }
        case SET_DATA:
        {
            ISetDataRequest requestRecord = (ISetDataRequest) ((Operation.RecordHolder<?>)request).asRecord();
            E node = asTrie().get(requestRecord.getPath());
            Long zxid = result.reply().zxid();
            update(node, StampedReference.of(zxid, requestRecord));
            Records.StatRecord responseRecord = (Records.StatRecord) ((Operation.RecordHolder<?>)response).asRecord();
            update(node, StampedReference.of(zxid, responseRecord));
            break;
        }
        default:
            changed = false;
            break;
        }
        return changed;
    }
    
    protected void update(E node, StampedReference<? extends Records.View> value) {
        for (View view: View.values()) {
            if (view.type().isInstance(value.get())) {
                StampedReference<? extends Records.View> prev = node.update(view, value);
                ViewUpdate event = ViewUpdate.ifUpdated(view, prev, value);
                if (event != null) {
                    post(event);
                }
            }
        }
    }
}
