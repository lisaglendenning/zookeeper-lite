package org.apache.zookeeper.data;

import com.google.common.base.Objects;

public class OpCallRequest implements Operation.RequestValue,
        Operation.CallRequest {

    public static OpCallRequest create(int xid, Operation.Request request) {
        return new OpCallRequest(xid, request);
    }

    protected int xid;
    protected Operation.Request request;

    public OpCallRequest(int xid, Operation.Request request) {
        super();
        this.xid = xid;
        this.request = request;
    }

    protected OpCallRequest() {
    }

    @Override
    public int xid() {
        return xid;
    }

    public OpCallRequest setXid(int xid) {
        this.xid = xid;
        return this;
    }

    @Override
    public Operation.Request request() {
        return request;
    }

    public OpCallRequest setRequest(Operation.Request request) {
        this.request = request;
        return this;
    }

    @Override
    public Operation operation() {
        return request().operation();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("xid", xid())
                .add("request", request()).toString();
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
        OpCallRequest other = (OpCallRequest) obj;
        return Objects.equal(xid(), other.xid())
                && Objects.equal(request(), other.request());
    }
}
