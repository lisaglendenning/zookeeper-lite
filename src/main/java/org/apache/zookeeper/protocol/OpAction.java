package org.apache.zookeeper.protocol;

import com.google.common.base.Objects;

public class OpAction implements Operation.Action {

    public static class Request extends OpAction implements Operation.Request {

        public static Request create(Operation operation) {
            return new Request(operation);
        }
        
        public Request(Operation operation) {
            super(operation);
        }
    }

    public static class Response extends OpAction implements Operation.Response {

        public static Response create(Operation operation) {
            return new Response(operation);
        }
        
        public Response(Operation operation) {
            super(operation);
        }
    }
        
    public static OpAction create(Operation operation) {
        return new OpAction(operation);
    }
    
    protected Operation operation;
        
    public OpAction(Operation operation) {
        super();
        this.operation = operation;
    }
    
    @Override
    public Operation operation() {
        return operation;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("operation", operation())
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(operation());
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
        OpAction other = (OpAction) obj;
        return Objects.equal(operation(), other.operation());
    }
}
