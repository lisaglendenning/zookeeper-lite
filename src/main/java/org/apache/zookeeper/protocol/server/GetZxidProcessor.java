package org.apache.zookeeper.protocol.server;

import org.apache.zookeeper.Zxid;
import org.apache.zookeeper.data.OpCallResponse;
import org.apache.zookeeper.data.OpCallResult;
import org.apache.zookeeper.data.Operation;
import org.apache.zookeeper.util.Processor;

public class GetZxidProcessor implements
        Processor<Operation.Response, Operation.Response> {

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
    public Operation.Response apply(Operation.Response response) {
        if ((response.operation() == Operation.CREATE_SESSION)
                || (response instanceof Operation.CallResponse)) {
            return response;
        }

        Operation.CallRequest callRequest = null;
        Operation.CallResponse callResponse = null;
        if (response instanceof Operation.CallRequest) {
            callRequest = (Operation.CallRequest) response;
        } else if (response instanceof Operation.Result) {
            Operation.Result result = (Operation.Result) response;
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
        return callResponse;
    }
}
