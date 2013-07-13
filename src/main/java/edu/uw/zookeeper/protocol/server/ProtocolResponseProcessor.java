package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.ProtocolResponseMessage;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;

public class ProtocolResponseProcessor implements Processor<TxnOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> {

    public static ProtocolResponseProcessor create(
            TxnRequestProcessor<Records.Request, Records.Response> processor) {
        return new ProtocolResponseProcessor(processor);
    }
    
    protected final RequestErrorProcessor<TxnOperation.Request<Records.Request>> delegate;
    
    public ProtocolResponseProcessor(
            TxnRequestProcessor<Records.Request, Records.Response> delegate) {
        this.delegate = RequestErrorProcessor.create(delegate);
    }
    
    @Override
    public Message.ServerResponse<Records.Response> apply(TxnOperation.Request<Records.Request> input) {
        Records.Response response = delegate.apply(input);

        int xid;
        if (response instanceof Operation.RequestId) {
            xid = ((Operation.RequestId) response).getXid();
        } else {
            xid = input.getXid();
        }
        long zxid = input.getZxid();
        return ProtocolResponseMessage.of(xid, zxid, response);
    }
}