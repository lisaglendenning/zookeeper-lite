package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Processors.ForwardingProcessor;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodeLabelVector;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IRemoveWatchesRequest;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class WatcherEventProcessor extends ForwardingProcessor<TxnOperation.Request<?>, Records.Response> implements Processors.CheckedProcessor<TxnOperation.Request<?>, Records.Response, KeeperException> {

    public static WatcherEventProcessor create(
            Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate,
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
    
    protected final Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate;
    protected final Watches dataWatches;
    protected final Watches childWatches;

    protected WatcherEventProcessor(
            Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate,
            Watches dataWatches,
            Watches childWatches) {
        this.delegate = delegate;
        this.dataWatches = dataWatches;
        this.childWatches = childWatches;
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<?> input) throws KeeperException {
        KeeperException exception = null;
        Records.Response response = null;
        if (input.record().opcode() == OpCode.REMOVE_WATCHES) {
            response = Operations.Responses.removeWatches().build();
        } else {
            try {
                response = delegate().apply(input);
            } catch (KeeperException e) {
                exception = e;
                response = null;
            }
        }
        return apply(input.getSessionId(), input.record(), response, exception);
    }
    
    protected Records.Response apply(Long session, Records.Request request, Records.Response response, KeeperException exception) throws KeeperException {
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
            if ((response != null) && !(response instanceof Operation.Error)) {
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
            if ((response != null) && !(response instanceof Operation.Error)) {
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
            if ((response != null) && !(response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) request).getPath();
                dataWatches.post(data(path));
            }
            break;
        }
        case MULTI:
        {
            Iterator<? extends Records.MultiOpRequest> requests = ((IMultiRequest) request).iterator();
            Iterator<? extends Records.MultiOpResponse> responses = (response != null) ? ((IMultiResponse) response).iterator() : Iterators.cycle(Operations.Responses.error().setError(exception.code()).build());
            while (requests.hasNext()) {
                apply(session, requests.next(), responses.next(), exception);
            }
            break;
        }
        case REMOVE_WATCHES:
        {
            String path = ((IRemoveWatchesRequest) request).getPath();
            List<Watches> types;
            switch (Watcher.WatcherType.fromInt(((IRemoveWatchesRequest) request).getType())) {
            case Any:
                types = ImmutableList.of(dataWatches, childWatches);
                break;
            case Children:
                types = ImmutableList.of(childWatches);
                break;
            case Data:
                types = ImmutableList.of(dataWatches);
                break;
            default:
                throw new AssertionError();
            }
            boolean removed = false;
            for (Watches watches: types) {
                if (watches.byPath().remove(path, session)) {
                    if (!removed) {
                        removed = true;
                    }
                }
            }
            if (!removed) {
                if (exception == null) {
                    exception = new KeeperException.NoWatcherException();
                }
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

        if (exception != null) {
            throw exception;
        } else {
            return response;
        }
    }

    @Override
    protected Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> delegate() {
        return delegate;
    }
}
