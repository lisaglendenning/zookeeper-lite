package edu.uw.zookeeper.server;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Processors.ForwardingProcessor;
import edu.uw.zookeeper.data.CreateFlag;
import edu.uw.zookeeper.data.CreateMode;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.TxnRequest;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolRequestMessage;
import edu.uw.zookeeper.protocol.SessionRequest;
import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class EphemeralProcessor extends ForwardingProcessor<TxnOperation.Request<?>, Records.Response> implements Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> {

    public static EphemeralProcessor create(
            Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> delegate) {
        return new EphemeralProcessor(delegate);
    }
    
    protected final Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> delegate;
    protected final SetMultimap<Long, String> bySession;
    protected final ConcurrentMap<String, Long> byPath;
    
    public EphemeralProcessor(
            Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> delegate) {
        this.delegate = delegate;
        this.bySession = Multimaps.synchronizedSetMultimap(HashMultimap.<Long, String>create());
        this.byPath = new MapMaker().makeMap();
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<?> input) {
        Records.Request request = input.record();
        Records.Response response = delegate().apply(input);
        Long session = input.getSessionId();
        if (request.opcode() == OpCode.CLOSE_SESSION) {
            if (! (response instanceof Operation.Error)) {
                for (String path: ImmutableSet.copyOf(bySession.get(session))) {
                    apply(TxnRequest.of(
                            input.getTime(), 
                            input.zxid(), 
                            SessionRequest.of(
                                    input.getSessionId(), 
                                    ProtocolRequestMessage.of(
                                            input.xid(),
                                            Operations.Requests.delete().setPath(ZNodePath.fromString(path)).build()))));
                }
            }
        } else {
            apply(session, request, response);
        }
        return response;
    }
    
    protected Records.Response apply(Long session, Records.Request request, Records.Response response) {
        switch (response.opcode()) {
        case CREATE:
        case CREATE2:
        {
            CreateMode mode = CreateMode.valueOf(((Records.CreateModeGetter) request).getFlags());
            if (mode.contains(CreateFlag.EPHEMERAL)) {
                String path = ((Records.PathGetter) response).getPath();
                bySession.put(session, path);
                byPath.put(path, session);
            }
        
            break;
        }
        case DELETE:
        {
            String path = ((Records.PathGetter) request).getPath();
            Long owner = byPath.remove(path);
            if (owner != null) {
                bySession.remove(owner, path);
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
            throw new AssertionError(request.toString());
        }
        default:
            break;
        }
        return response;
    }
    
    @Override
    protected Processors.UncheckedProcessor<TxnOperation.Request<?>, Records.Response> delegate() {
        return delegate;
    }
}
