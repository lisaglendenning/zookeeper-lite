package edu.uw.zookeeper.protocol;

import edu.uw.zookeeper.common.AbstractPair;
import edu.uw.zookeeper.protocol.Operation;

public class OperationResult<T extends Operation.Request, V extends Operation.Response> extends AbstractPair<T,V> implements Operation.Result<T,V> {

    public static <T extends Operation.Request, V extends Operation.Response> 
    OperationResult<T,V> create(T request, V response) {
        return new OperationResult<T,V>(request, response);
    }
    
    public OperationResult(T request, V response) {
        super(request, response);
    }

    @Override
    public T getRequest() {
        return first;
    }

    @Override
    public V getResponse() {
        return second;
    }
}
