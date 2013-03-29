package org.apache.zookeeper.protocol.server;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.protocol.OpCallResult;
import org.apache.zookeeper.protocol.OpCallResponse;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.util.PipeProcessor;

import com.google.common.base.Optional;

public class GetZxidProcessor implements PipeProcessor<Operation.Response> {

    public static GetZxidProcessor create() {
        return new GetZxidProcessor(Zxid.create());
    }

    public static GetZxidProcessor create(Zxid zxid) {
        return new GetZxidProcessor(zxid);
    }

    protected final Zxid zxid;

    protected GetZxidProcessor(Zxid zxid) {
        this.zxid = zxid;
    }
    
    public Zxid zxid() {
        return zxid;
    }

    @Override
    public Optional<Operation.Response> apply(Operation.Response response) {
        if ((response.operation() == Operation.CREATE_SESSION) 
                || (response instanceof Operation.CallResponse)) {
            return Optional.of(response);
        }
        
        Operation.CallRequest callRequest = null;
        Operation.CallResponse callResponse = null;
        if (response instanceof Operation.CallRequest) {
            callRequest = (Operation.CallRequest)response;
        } else if (response instanceof Operation.Result) {
            Operation.Result result = (Operation.Result)response;
            Operation.Request request = (Operation.Request) result.request();
            if (request instanceof Operation.CallRequest) {
                callRequest = (Operation.CallRequest) request;
                response = (Operation.Response) result.response();
            }
        }

        long zxid = zxid().get();
        callResponse = OpCallResponse.create(zxid, response);
        if (callRequest != null) {
            callResponse = OpCallResult.create(callRequest, callResponse);
        }
        return Optional.<Operation.Response>of(callResponse);
    }
}
