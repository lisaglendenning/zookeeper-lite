package edu.uw.zookeeper.data;


import java.util.Collections;
import java.util.Map;
import org.apache.jute.Record;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.ClientProtocolConnection;
import edu.uw.zookeeper.protocol.proto.IGetACLResponse;
import edu.uw.zookeeper.protocol.proto.IGetDataResponse;
import edu.uw.zookeeper.protocol.proto.ISetACLRequest;
import edu.uw.zookeeper.protocol.proto.ISetDataRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.proto.Records.ChildrenRecord;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.Eventful;
import edu.uw.zookeeper.util.ForwardingEventful;
import edu.uw.zookeeper.util.ForwardingPromise;
import edu.uw.zookeeper.util.Promise;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.SettableFuturePromise;
import edu.uw.zookeeper.util.TaskExecutor;

public class ZNodeCacheTrie extends ForwardingEventful implements TaskExecutor<Operation.Request, Operation.SessionResult>, Eventful {

    public static class ZNodeCache extends ZNodeLabelTrie.AbstractNode<ZNodeCache> {

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
        
        public static ZNodeCache root() {
            return new ZNodeViewFactory().get();
        }

        public static ZNodeCache childOf(ZNodeLabelTrie.Pointer<ZNodeCache> parent) {
            return new ZNodeViewFactory().get(parent);
        }
        
        public static class ZNodeViewFactory implements DefaultsFactory<ZNodeLabelTrie.Pointer<ZNodeCache>, ZNodeCache> {

            public ZNodeViewFactory() {}
            
            @Override
            public ZNodeCache get() {
                return new ZNodeCache(Optional.<ZNodeLabelTrie.Pointer<ZNodeCache>>absent(), this);
            }

            @Override
            public ZNodeCache get(ZNodeLabelTrie.Pointer<ZNodeCache> value) {
                return new ZNodeCache(Optional.of(value), this);
            }
            
        }

        protected final Map<View, StampedReference.Updater<? extends Records.View>> views;
        
        protected ZNodeCache(
                Optional<ZNodeLabelTrie.Pointer<ZNodeCache>> parent,
                DefaultsFactory<ZNodeLabelTrie.Pointer<ZNodeCache>, ZNodeCache> factory) {
            super(parent, factory);
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
        
        public void update(StampedReference<? extends Records.View> value) {
            for (View view: View.values()) {
                if (view.type().isInstance(value.get())) {
                    update(view, value);
                }
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
            return Objects.toStringHelper(this).add("path", path()).add("views", views).toString();
        }
    }
    
    protected class PromiseWrapper extends ForwardingPromise<Operation.SessionResult> {

        protected final Promise<Operation.SessionResult> delegate;
        
        protected PromiseWrapper() {
            this.delegate = SettableFuturePromise.<Operation.SessionResult>create();
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
    
    protected final ZNodeLabelTrie<ZNodeCache> trie;
    protected final ClientProtocolConnection client;
    
    protected ZNodeCacheTrie(Publisher publisher, ClientProtocolConnection client) {
        super(publisher);
        this.trie = ZNodeLabelTrie.of(ZNodeCache.root());
        this.client = client;
        client.register(this);
    }
    
    public ZNodeLabelTrie<ZNodeCache> asTrie() {
        return trie;
    }

    @Override
    public ClientProtocolConnection.RequestFuture submit(Operation.Request request) {
        // wrapper so that we can apply changes before our client sees them
        return client.submit(request, new PromiseWrapper());
    }
    
    protected void handleResult(Operation.SessionResult result) {
        if (! (result.reply().reply() instanceof Operation.Response)) {
            // no updates to apply
            return;
        }
        Operation.Response response = (Operation.Response) (result.reply().reply());
        Operation.Request request = result.request().request();
        
        switch (request.opcode()) {
        case CREATE:
        case CREATE2:
        {
            Record responseRecord = ((Operation.RecordHolder<?>)response).asRecord();
            ZNodeCache node = asTrie().put(((Records.PathHolder)responseRecord).getPath());
            Long zxid = result.reply().zxid();
            Records.CreateRecord record = (Records.CreateRecord)((Operation.RecordHolder<?>)request).asRecord();
            StampedReference<Records.CreateRecord> stampedRequest = StampedReference.of(zxid, record);
            node.update(stampedRequest);
            if (responseRecord instanceof Records.StatRecord) {
                StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                node.update(stampedResponse);
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
            ZNodeCache node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            Long zxid = result.reply().zxid();
            StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)((Operation.RecordHolder<?>)response).asRecord());
            node.update(stampedResponse);
            break;
        }
        case GET_ACL:
        {
            ZNodeCache node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            StampedReference<IGetACLResponse> stampedResponse = StampedReference.of(
                    result.reply().zxid(), (IGetACLResponse)((Operation.RecordHolder<?>)response).asRecord());
            node.update(stampedResponse);
            break;
        }
        case GET_CHILDREN:
        case GET_CHILDREN2:        
        {
            ZNodeCache node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            Records.ChildrenRecord responseRecord = (ChildrenRecord) ((Operation.RecordHolder<?>)response).asRecord();
            for (String child: responseRecord.getChildren()) {
                node.put(child);
            }
            if (responseRecord instanceof Records.StatRecord) {
                Long zxid = result.reply().zxid();
                StampedReference<Records.StatRecord> stampedResponse = StampedReference.of(zxid, (Records.StatRecord)responseRecord);
                node.update(stampedResponse);
            }
            break;
        }
        case GET_DATA:
        {
            ZNodeCache node = asTrie().get(((Records.PathHolder)((Operation.RecordHolder<?>)request).asRecord()).getPath());
            StampedReference<IGetDataResponse> stampedResponse = StampedReference.of(
                    result.reply().zxid(), (IGetDataResponse)((Operation.RecordHolder<?>)response).asRecord());
            node.update(stampedResponse);
            break;
        }
        case MULTI:
        {
            // TODO
            break;
        }
        case SET_ACL:
        {
            ISetACLRequest requestRecord = (ISetACLRequest) ((Operation.RecordHolder<?>)request).asRecord();
            ZNodeCache node = asTrie().get(requestRecord.getPath());
            Long zxid = result.reply().zxid();
            node.update(StampedReference.of(zxid, requestRecord));
            Records.StatRecord responseRecord = (Records.StatRecord) ((Operation.RecordHolder<?>)response).asRecord();
            node.update(StampedReference.of(zxid, responseRecord));
            break;
        }
        case SET_DATA:
        {
            ISetDataRequest requestRecord = (ISetDataRequest) ((Operation.RecordHolder<?>)request).asRecord();
            ZNodeCache node = asTrie().get(requestRecord.getPath());
            Long zxid = result.reply().zxid();
            node.update(StampedReference.of(zxid, requestRecord));
            Records.StatRecord responseRecord = (Records.StatRecord) ((Operation.RecordHolder<?>)response).asRecord();
            node.update(StampedReference.of(zxid, responseRecord));
            break;
        }
        default:
            break;
        }
    }
}
