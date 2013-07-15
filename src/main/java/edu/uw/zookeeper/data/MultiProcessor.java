package edu.uw.zookeeper.data;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.protocol.proto.IMultiRequest;
import edu.uw.zookeeper.protocol.proto.IMultiResponse;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.TxnRequestProcessor;

// TODO
public class MultiProcessor implements TxnRequestProcessor<IMultiRequest, IMultiResponse> {

    protected final TxnRequestProcessor<Records.Request, Records.Response> delegate;
    
    public MultiProcessor(TxnRequestProcessor<Records.Request, Records.Response> delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public IMultiResponse apply(TxnOperation.Request<IMultiRequest> request) throws KeeperException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }
    
    protected TxnRequestProcessor<Records.Request, Records.Response> delegate() {
        return delegate;
    }
}
