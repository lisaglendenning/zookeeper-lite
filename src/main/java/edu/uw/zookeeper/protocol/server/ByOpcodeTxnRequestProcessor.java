package edu.uw.zookeeper.protocol.server;

import java.util.Map;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class ByOpcodeTxnRequestProcessor implements TxnRequestProcessor<Records.Request, Records.Response> {

    public static ByOpcodeTxnRequestProcessor create(
            ImmutableMap<OpCode, ? extends TxnRequestProcessor<?,?>> processors) {
        return new ByOpcodeTxnRequestProcessor(processors);
    }

    @SuppressWarnings("unchecked")
    public static <T extends Records.Request, V extends Records.Response> V apply(TxnRequestProcessor<T,V> processor, TxnOperation.Request<?> request) throws KeeperException {
        return processor.apply((TxnOperation.Request<T>) request);
    }

    protected final Map<OpCode, ? extends TxnRequestProcessor<?,?>> processors;
    
    protected ByOpcodeTxnRequestProcessor(Map<OpCode, ? extends TxnRequestProcessor<?,?>> processors) {
        this.processors = processors;
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<Records.Request> input) throws KeeperException {
        TxnRequestProcessor<?,?> processor = processors.get(input.getRecord().getOpcode());
        if (processor == null) {
            throw new IllegalArgumentException(input.toString());
        }
        return apply(processor, input);
    }
}
