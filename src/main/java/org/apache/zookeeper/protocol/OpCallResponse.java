package org.apache.zookeeper.protocol;

import com.google.common.base.Objects;

public class OpCallResponse<T extends Operation.Response> implements Operation.ResponseValue<T>, Operation.CallResponse {

    public static <T extends Operation.Response> Operation.CallResponse create(long zxid, T response) {
        return new OpCallResponse<T>(zxid, response);
    }

    protected long zxid;
    protected T response;
    
    public OpCallResponse(long zxid, T response) {
        super();
        this.zxid = zxid;
        this.response = response;
    }

    protected OpCallResponse() {}
    
    @Override
    public long zxid() {
        return zxid;
    }

    public OpCallResponse<T> setZxid(long zxid) {
        this.zxid = zxid;
        return this;
    }

    @Override
    public Operation operation() {
        return response.operation();
    }
 
    @Override
    public T response() {
        return response;
    }

    public OpCallResponse<T> setResponse(T response) {
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
        @SuppressWarnings("unchecked")
        OpCallResponse<T> other = (OpCallResponse<T>) obj;
        return Objects.equal(zxid(), other.zxid()) 
                && Objects.equal(response(), other.response());
    }
}
