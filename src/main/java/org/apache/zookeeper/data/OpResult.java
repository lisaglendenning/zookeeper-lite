package org.apache.zookeeper.data;


import com.google.common.base.Objects;

public class OpResult implements Operation.Result {

    public static Operation.Result create(Operation.Request request, Operation.Response response) {
        return new OpResult(request, response);
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
