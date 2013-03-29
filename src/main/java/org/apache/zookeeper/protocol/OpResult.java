package org.apache.zookeeper.protocol;

import com.google.common.base.Objects;

public class OpResult<T extends Operation.Request, V> implements Operation.Result<T, V> {

    public static <T extends Operation.Request, V> Operation.Result<T,V> create(T request, V result) {
        return new OpResult<T,V>(request, result);
    }
    
    protected T request;
    protected V response;
    
    public OpResult(T request, V response) {
        super();
        this.request = request;
        this.response = response;
    }

    @Override
    public Operation operation() {
        return request().operation();
    }

    @Override
    public V response() {
        return response;
    }

    @Override
    public T request() {
        return request;
    }

    public void setRequest(T request) {
        this.request = request;
    }

    public void setResponse(V response) {
        this.response = response;
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
        @SuppressWarnings("unchecked")
        OpResult<T,V> other = (OpResult<T,V>) obj;
        return Objects.equal(request(), other.request()) 
                && Objects.equal(response(), other.response());
    }
}
