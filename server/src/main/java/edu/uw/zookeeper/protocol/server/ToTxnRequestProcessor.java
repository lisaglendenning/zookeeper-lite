package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.common.Processors;
import edu.uw.zookeeper.common.Reference;
import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.TxnRequest;
import edu.uw.zookeeper.protocol.SessionOperation;

public class ToTxnRequestProcessor implements Processors.UncheckedProcessor<SessionOperation.Request<?>, TxnOperation.Request<?>>, Reference<AssignZxidProcessor> {

    public static ToTxnRequestProcessor create() {
        return create(AssignZxidProcessor.newInstance());
    }

    public static ToTxnRequestProcessor create(AssignZxidProcessor zxids) {
        return new ToTxnRequestProcessor(zxids);
    }
    
    public static long getTime() {
        return System.currentTimeMillis();
    }
    
    protected final AssignZxidProcessor zxids;
    
    public ToTxnRequestProcessor(AssignZxidProcessor zxids) {
        this.zxids = zxids;
    }
    
    @Override
    public AssignZxidProcessor get() {
        return zxids;
    }

    @Override
    public TxnOperation.Request<?> apply(SessionOperation.Request<?> input) {
        long time = getTime();
        long zxid = get().apply(input.record().opcode());
        return TxnRequest.of(time, zxid, input);
    }
}