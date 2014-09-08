package edu.uw.zookeeper.server;

import java.util.Iterator;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Processors.ForwardingProcessor;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.Records;

public class WatcherEventProcessor extends ForwardingProcessor<TxnOperation.Request<?>, Records.Response> implements Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> {

    public static WatcherEventProcessor create(
            Processors.UncheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response> delegate,
            Watches dataWatches,
            Watches childWatches) {
        return new WatcherEventProcessor(delegate, dataWatches, childWatches);
    }

    public static IWatcherEvent created(String path) {
        return of(EventType.NodeCreated.getIntValue(), KeeperState.SyncConnected.getIntValue(), path);
    }

    public static IWatcherEvent deleted(String path) {
        return of(EventType.NodeDeleted.getIntValue(), KeeperState.SyncConnected.getIntValue(), path);
    }

    public static IWatcherEvent data(String path) {
        return of(EventType.NodeDataChanged.getIntValue(), KeeperState.SyncConnected.getIntValue(), path);
    }

    public static IWatcherEvent children(String path) {
        return of(EventType.NodeChildrenChanged.getIntValue(), KeeperState.SyncConnected.getIntValue(), path);
    }
    
    public static IWatcherEvent of(int type, int state, String path) {
        return new IWatcherEvent(type, state, path);
    }
    
    protected final Processors.UncheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response> delegate;
    protected final Watches dataWatches;
    protected final Watches childWatches;

    public WatcherEventProcessor(
            Processors.UncheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response> delegate,
            Watches dataWatches,
            Watches childWatches) {
        this.delegate = delegate;
        this.dataWatches = dataWatches;
        this.childWatches = childWatches;
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<?> input) {
        return apply(input.getSessionId(), input.record(), delegate().apply(input));
    }
    
    protected Records.Response apply(Long session, Records.Request request, Records.Response response) {
        switch (request.opcode()) {
        case GET_DATA:
        case EXISTS:
            if (((Records.WatchGetter) request).getWatch()) {
                dataWatches.put(session, ((Records.PathGetter) request).getPath());
            }
            break;
        case GET_CHILDREN:
        case GET_CHILDREN2:
            if (((Records.WatchGetter) request).getWatch()) {
                childWatches.put(session, ((Records.PathGetter) request).getPath());
            }
            break;
        case CREATE:
        case CREATE2:
        {
            if (! (response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) response).getPath();
                String parent = ZNodeLabelVector.headOf(path);
                dataWatches.post(created(path));
                if (parent.length() > 0) {
                    childWatches.post(children(parent));
                }
            }
            break;
        }
        case DELETE:
        {
            if (! (response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) request).getPath();
                String parent = ZNodeLabelVector.headOf(path);
                dataWatches.post(deleted(path));
                if (parent.length() > 0) {
                    childWatches.post(children(parent));
                }
            }
            break;
        }
        case SET_DATA:
        {
            if (! (response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) request).getPath();
                dataWatches.post(data(path));
            }
            break;
        }
        case MULTI:
        {
            Iterator<Records.MultiOpRequest> requests = ((IMultiRequest) request).iterator();
            Iterator<Records.MultiOpResponse> responses = ((IMultiResponse) response).iterator();
            while (requests.hasNext()) {
                apply(session, requests.next(), responses.next());
            }
            break;
        }
        case CLOSE_SESSION:
        {
            dataWatches.remove(session);
            childWatches.remove(session);
            break;
        }
        default:
            break;
        }
        
        return response;
    }

    @Override
    protected Processors.UncheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response> delegate() {
        return delegate;
    }
}
