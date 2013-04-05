package org.apache.zookeeper.data;


import static com.google.common.base.Preconditions.checkArgument;

import org.apache.zookeeper.KeeperException.Code;

import com.google.common.base.Objects;

public class OpResult implements Operation.Result {

    public static Operation.Result create(Operation.Request request, Operation.Response response) {
        Operation.Result result;
        if (request instanceof Operation.CallRequest && response instanceof Operation.CallResponse) {
        	result = OpCallResult.create((Operation.CallRequest) request,
        			(Operation.CallResponse) response);
        } else if (response instanceof Operation.Error) {
        	result = new OpResultError(request, response);
        } else {
        	result = new OpResult(request, response);
        }
        return result;
    }

    public static class OpResultError extends OpResult implements Operation.Error {

        protected OpResultError(Operation.Request request, Operation.Response response) {
            super(request, response);
            checkArgument(response instanceof Operation.Error);
        }

		@Override
        public Code error() {
	        return ((Operation.Error) response).error();
        }
    	
    }

    protected final Operation.Request request;
    protected final Operation.Response response;
    
    public OpResult(Operation.Request request, Operation.Response response) {
        super();
        this.request = request;
        this.response = response;
    }

    @Override
    public Operation operation() {
        return request().operation();
    }

    @Override
    public Operation.Request request() {
        return request;
    }

    @Override
    public Operation.Response response() {
        return response;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("request", request())
                .add("response", response())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(request(), response());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OpResult other = (OpResult) obj;
        return Objects.equal(request(), other.request()) 
                && Objects.equal(response(), other.response());
    }
}
