package edu.uw.zookeeper.data;

import java.util.concurrent.ConcurrentMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionOperationRequest;
import edu.uw.zookeeper.protocol.proto.IDeleteRequest;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.TxnRequestProcessor;
import edu.uw.zookeeper.util.Processors.ForwardingProcessor;

public class EphemeralProcessor extends ForwardingProcessor<TxnOperation.Request<Records.Request>, Records.Response> implements TxnRequestProcessor<Records.Request,Records.Response> {

    public static EphemeralProcessor newInstance(TxnRequestProcessor<Records.Request, Records.Response> delegate) {
        return new EphemeralProcessor(delegate);
    }
    
    protected final TxnRequestProcessor<Records.Request, Records.Response> delegate;
    protected final SetMultimap<Long, String> bySession;
    protected final ConcurrentMap<String, Long> byPath;
    
    public EphemeralProcessor(
            TxnRequestProcessor<Records.Request, Records.Response> delegate) {
        this.delegate = delegate;
        this.bySession = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, String>create());
        this.byPath = new MapMaker().makeMap();
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<Records.Request> input) throws KeeperException {
        Records.Response output = delegate().apply(input);
        switch (output.getOpcode()) {
        case CREATE:
        case CREATE2:
        {
            if (CreateMode.fromFlag(((Records.CreateModeGetter) input.getRecord()).getFlags()).isEphemeral()) {
                Long session = input.getSessionId();
                String path = ((Records.PathGetter) output).getPath();
                bySession.put(session, path);
                byPath.put(path, session);
            }
            break;
        }
        case CLOSE_SESSION:
        {
            Long session = input.getSessionId();
            for (String path: bySession.get(session)) {
                ProtocolRequestMessage<Records.Request> nested = ProtocolRequestMessage.of(input.getXid(), (Records.Request) new IDeleteRequest(path, Stats.VERSION_ANY));
                apply(TxnRequest.of(input.getTime(), input.getZxid(), SessionOperationRequest.of(session, nested, nested)));
            }
            break;
        }
        case DELETE:
        {
            String path = ((Records.PathGetter) input.getRecord()).getPath();
            Long session = byPath.remove(path);
            if (session != null) {
                bySession.remove(session, path);
            }
            break;
        }
        default:
            break;
        }
        return output;
    }
    
    @Override
    protected TxnRequestProcessor<Records.Request, Records.Response> delegate() {
        return delegate;
    }
}
