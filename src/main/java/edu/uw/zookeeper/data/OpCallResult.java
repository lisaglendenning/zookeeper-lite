package edu.uw.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.zookeeper.KeeperException.Code;

public class OpCallResult extends OpResult implements Operation.CallResult {

    public static Operation.CallResult create(Operation.CallRequest request,
            Operation.CallResponse response) {
        Operation.CallResult callResult;
        if (response instanceof Operation.Error) {
            callResult = new OpCallResultError(request, response);
        } else {
            callResult = new OpCallResult(request, response);
        }
        return callResult;
    }

    public OpCallResult(Operation.CallRequest request,
            Operation.CallResponse response) {
        super(request, response);
    }

    public static class OpCallResultError extends OpCallResult implements
            Operation.Error {
        protected OpCallResultError(Operation.CallRequest request,
                Operation.CallResponse response) {
            super(request, response);
            checkArgument(response instanceof Operation.Error);
        }

        @Override
        public Code error() {
            return ((Operation.Error) response).error();
        }
    }

    @Override
    public Operation.CallRequest request() {
        return (Operation.CallRequest) request;
    }

    @Override
    public Operation.CallResponse response() {
        return (Operation.CallResponse) response;
    }

    @Override
    public long zxid() {
        return response().zxid();
    }

    @Override
    public int xid() {
        return request().xid();
    }
}
