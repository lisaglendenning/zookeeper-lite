package org.apache.zookeeper.protocol;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;

public class OpError implements Operation.Error, Operation.ResponseValue {

    public static OpError create(Operation.Response response, KeeperException.Code error) {
        return new OpError(response, error);
    }
    
    protected Operation.Response response;
    protected KeeperException.Code error;
        
    public OpError(Operation.Response response, KeeperException.Code error) {
        super();
        this.response = response;
        this.error = error;
    }
    
    @Override
    public Operation operation() {
        return response().operation();
    }
    
    @Override
    public Operation.Response response() {
        return response;
    }

    @Override
    public KeeperException.Code error() {
        return error;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("response", response())
                .add("error", error())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(response(), error());
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
        OpError other = (OpError) obj;
        return Objects.equal(response(), other.response()) 
                && Objects.equal(error(), other.error());
    }

}
