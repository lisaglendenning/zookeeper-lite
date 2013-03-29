package org.apache.zookeeper.protocol;

import com.google.common.base.Objects;

public class OpCallRequest<T extends Operation.Request> implements Operation.RequestValue<T>, Operation.CallRequest {

    public static <T extends Operation.Request> OpCallRequest<T> create(int xid, T request) {
        return new OpCallRequest<T>(xid, request);
    }
    
    protected int xid;
    protected T request;
    
    public OpCallRequest(int xid, T request) {
        super();
        this.xid = xid;
        this.request = request;
    }
    
    protected OpCallRequest() {}

    @Override
    public int xid() {
        return xid;
    }

    public OpCallRequest<T> setXid(int xid) {
        this.xid = xid;
        return this;
    }

    @Override
    public T request() {
        return request;
    }

    public OpCallRequest<T> setRequest(T request) {
        this.request = request;
        return this;
    }
    
    @Override
    public Operation operation() {
        return request().operation();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("xid", xid())
                .add("request", request())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(xid(), request());
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
        OpCallRequest<T> other = (OpCallRequest<T>) obj;
        return Objects.equal(xid(), other.xid()) 
                && Objects.equal(request(), other.request());
    }
}
