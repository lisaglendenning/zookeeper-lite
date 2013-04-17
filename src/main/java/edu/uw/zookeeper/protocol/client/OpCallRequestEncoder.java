package edu.uw.zookeeper.protocol.client;

import java.io.IOException;
import java.io.OutputStream;

import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.protocol.Encodable;
import edu.uw.zookeeper.protocol.Encoder;
import edu.uw.zookeeper.protocol.Records;

public class OpCallRequestEncoder implements Encoder<Operation.CallRequest> {

    public static OpCallRequestEncoder create() {
        return new OpCallRequestEncoder();
    }

    public OpCallRequestEncoder() {
    }

    @SuppressWarnings("rawtypes")
    @Override
    public OutputStream encode(Operation.CallRequest callRequest,
            OutputStream stream) throws IOException {
        Records.Requests.Headers.serialize(callRequest.xid(),
                callRequest.operation(), stream);
        // unravel the layers...
        Operation.Request request = callRequest;
        while (request instanceof Operation.RequestValue) {
            Operation.RequestValue nextRequest = (Operation.RequestValue) request;
            if (nextRequest.request() instanceof Operation.Request) {
                request = (Operation.Request) nextRequest.request();
            } else {
                break;
            }
        }
        if (request instanceof Encodable) {
            stream = ((Encodable) request).encode(stream);
        }
        return stream;
    }
}