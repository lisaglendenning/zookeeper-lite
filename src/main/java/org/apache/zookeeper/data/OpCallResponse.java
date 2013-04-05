package org.apache.zookeeper.data;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.zookeeper.KeeperException.Code;

import com.google.common.base.Objects;


public class OpCallResponse implements Operation.ResponseValue, Operation.CallResponse {

    public static Operation.CallResponse create(long zxid, Operation.Response response) {
    	Operation.CallResponse callResponse;
    	if (response instanceof Operation.Error) {
    		callResponse = new OpCallResponseError(zxid, response);
    	} else {
    		callResponse = new OpCallResponse(zxid, response);
    	}
    	return callResponse;
    }
    
    public static class OpCallResponseError extends OpCallResponse implements Operation.Error {
        protected OpCallResponseError(long zxid, Operation.Response response) {
            super(zxid, response);
            checkArgument(response instanceof Operation.Error);
        }

		@Override
        public Code error() {
	        return ((Operation.Error) response).error();
        }
    }

    protected long zxid;
    protected Operation.Response response;
    
    protected OpCallResponse(long zxid, Operation.Response response) {
        super();
        this.zxid = zxid;
        this.response = response;
    }

    protected OpCallResponse() {}
    
    @Override
    public long zxid() {
        return zxid;
    }

    public OpCallResponse setZxid(long zxid) {
        this.zxid = zxid;
        return this;
    }

    @Override
    public Operation operation() {
        return response.operation();
    }
 
    @Override
    public Operation.Response response() {
        return response;
    }

    public OpCallResponse setResponse(Operation.Response response) {
        this.response = response;
        return this;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("zxid", zxid())
                .add("response", response())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(zxid(), response());
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
        OpCallResponse other = (OpCallResponse) obj;
        return Objects.equal(zxid(), other.zxid()) 
                && Objects.equal(response(), other.response());
    }
}
