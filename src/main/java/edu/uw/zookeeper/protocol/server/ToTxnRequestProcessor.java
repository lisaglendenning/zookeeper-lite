package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.data.TxnRequest;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Reference;

public class ToTxnRequestProcessor implements Processor<SessionOperation.Request<Records.Request>, TxnOperation.Request<Records.Request>>, Reference<AssignZxidProcessor> {

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
    public TxnOperation.Request<Records.Request> apply(SessionOperation.Request<Records.Request> input) {
        long time = getTime();
        long zxid = get().apply(input.getRecord().getOpcode());
        return TxnRequest.of(time, zxid, input);
    }
}