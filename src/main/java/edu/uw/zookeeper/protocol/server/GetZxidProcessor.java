package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.ZxidCounter;
import edu.uw.zookeeper.data.OpCallResponse;
import edu.uw.zookeeper.data.OpCallResult;
import edu.uw.zookeeper.data.Operation;
import edu.uw.zookeeper.util.Processor;

public class GetZxidProcessor implements
        Processor<Operation.Response, Operation.Response> {

    public static GetZxidProcessor create() {
        return new GetZxidProcessor(ZxidCounter.create());
    }

    public static GetZxidProcessor create(ZxidCounter zxid) {
        return new GetZxidProcessor(zxid);
    }

    protected final ZxidCounter zxid;

    protected GetZxidProcessor(ZxidCounter zxid) {
        this.zxid = zxid;
    }

    public ZxidCounter zxid() {
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
