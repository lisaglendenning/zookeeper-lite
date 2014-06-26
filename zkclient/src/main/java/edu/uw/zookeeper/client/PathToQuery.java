package edu.uw.zookeeper.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Function;

import edu.uw.zookeeper.client.ClientExecutor;
import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.data.Operations;
import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

public final class PathToQuery<I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> extends AbstractPair<I, ClientExecutor<? super Records.Request,O,?>> implements Function<ZNodePath, FixedQuery<O>> {

    public static <O extends Operation.ProtocolResponse<?>> PathToQuery<PathToRequests, O> forRequests(ClientExecutor<? super Records.Request,O,?> client, Operations.Builder<? extends Records.Request>... builders) {
        return forFunction(client, PathToRequests.forRequests(builders));
    }
    
    public static <O extends Operation.ProtocolResponse<?>> PathToQuery<PathToRequests, O> forIterable(ClientExecutor<? super Records.Request,O,?> client, Iterable<? extends Operations.Builder<? extends Records.Request>> builders) {
        return forFunction(client, PathToRequests.forIterable(builders));
    }

    public static <I extends Function<ZNodePath, ? extends List<? extends Records.Request>>, O extends Operation.ProtocolResponse<?>> PathToQuery<I, O> forFunction(ClientExecutor<? super Records.Request,O,?> client, I requests) {
        return new PathToQuery<I, O>(requests, client);
    }
    
    protected PathToQuery(I requests, ClientExecutor<? super Records.Request,O,?> client) {
        super(checkNotNull(requests), checkNotNull(client));
    }
    
    public I requests() {
        return first;
    }
    
    public ClientExecutor<? super Records.Request,O,?> client() {
        return second;
    }
    
    @Override
    public FixedQuery<O> apply(final ZNodePath path) {
        return FixedQuery.forIterable(client(), requests().apply(path));
    }
}