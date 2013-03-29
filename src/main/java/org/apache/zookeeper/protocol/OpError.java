package org.apache.zookeeper.protocol;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;

public class OpError<T extends Operation.Response> implements Operation.Error, Operation.ResponseValue<T> {

    public static <T extends Operation.Response> OpError<T> create(T action, KeeperException.Code error) {
        return new OpError<T>(action, error);
    }
    
    protected T response;
    protected KeeperException.Code error;
        
    public OpError(T response, KeeperException.Code error) {
        super();
        this.response = response;
        this.error = error;
    }
    
    @Override
    public Operation operation() {
        return response().operation();
    }
    
    @Override
    public T response() {
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
    
    @SuppressWarnings("unchecked")
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
        OpError<T> other = (OpError<T>) obj;
        return Objects.equal(response(), other.response()) 
                && Objects.equal(error(), other.error());
    }

}
