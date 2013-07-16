package edu.uw.zookeeper.protocol.server;

import java.util.Iterator;
import java.util.Set;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.IWatcherEvent;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processors.ForwardingProcessor;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.Processors;

public class WatcherEventProcessor extends ForwardingProcessor<TxnOperation.Request<Records.Request>, Records.Response> implements Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> {

    public static WatcherEventProcessor create(
            Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> delegate,
            Function<Long, Publisher> publishers) {
        return new WatcherEventProcessor(delegate, publishers);
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
    
    public static String parentOf(String path) {
        int lastSlash = path.indexOf(ZNodeLabel.SLASH);
        return (lastSlash > 0) ? path.substring(0, lastSlash) : "";
    }
    
    protected final Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> delegate;
    protected final Watches dataWatches;
    protected final Watches childWatches;

    public WatcherEventProcessor(
            Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> delegate,
            Function<Long, Publisher> publishers) {
        this.delegate = delegate;
        this.dataWatches = new Watches(publishers);
        this.childWatches = new Watches(publishers);
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<Records.Request> input) {
        return apply(input.getSessionId(), input.getRecord(), delegate().apply(input));
    }
    
    protected Records.Response apply(Long session, Records.Request request, Records.Response response) {
        switch (request.getOpcode()) {
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
        case DELETE:
        {
            if (! (response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) response).getPath();
                String parent = parentOf(path);
                dataWatches.post((response.getOpcode() == OpCode.DELETE) ? deleted(path) : created(path));
                if (parent.length() > 0) {
                    childWatches.post(children(parent));
                }
            }
            break;
        }
        case SET_DATA:
        {
            if (! (response instanceof Operation.Error)) {
                String path = ((Records.PathGetter) response).getPath();
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
    protected Processors.UncheckedProcessor<TxnOperation.Request<Records.Request>, Records.Response> delegate() {
        return delegate;
    }
    
    public static class Watches {

        protected final SetMultimap<String, Long> byPath;
        protected final SetMultimap<Long, String> bySession;
        protected final Function<Long, Publisher> publishers;
        
        public Watches(
                Function<Long, Publisher> publishers) {
            this.byPath = Multimaps.synchronizedSetMultimap(HashMultimap.<String, Long>create());
            this.bySession = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, String>create());
            this.publishers = publishers;
        }
        
        public void post(IWatcherEvent event) {
            String path = event.getPath();
            for (Long session: byPath.removeAll(path)) {
                bySession.remove(session, path);
                Publisher publisher = publishers.apply(session);
                if (publisher != null) {
                    publisher.post(event);
                }
            }
        }
        
        public void put(Long session, String path) {
            byPath.put(path, session);
            bySession.put(session, path);
        }
        
        public Set<String> remove(Long session) {
            Set<String> paths = bySession.removeAll(session);
            for (String path: paths) {
                byPath.remove(path, session);
            }
            return paths;
        }
    }
}
