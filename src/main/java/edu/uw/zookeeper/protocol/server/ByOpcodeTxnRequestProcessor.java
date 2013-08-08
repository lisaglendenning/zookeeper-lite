package edu.uw.zookeeper.protocol.server;

import java.util.Map;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.proto.OpCode;
import edu.uw.zookeeper.protocol.proto.Records;

public class ByOpcodeTxnRequestProcessor implements Processors.CheckedProcessor<TxnOperation.Request<?>, Records.Response, KeeperException> {

    public static ByOpcodeTxnRequestProcessor create(
            ImmutableMap<OpCode, ? extends Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> processors) {
        return new ByOpcodeTxnRequestProcessor(processors);
    }

    protected final Map<OpCode, ? extends Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> processors;
    
    protected ByOpcodeTxnRequestProcessor(Map<OpCode, ? extends Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException>> processors) {
        this.processors = processors;
    }
    
    @Override
    public Records.Response apply(TxnOperation.Request<?> input) throws KeeperException {
        Processors.CheckedProcessor<TxnOperation.Request<?>, ? extends Records.Response, KeeperException> processor = processors.get(input.getRecord().getOpcode());
        if (processor == null) {
            throw new IllegalArgumentException(input.toString());
        }
        return processor.apply(input);
    }
}
