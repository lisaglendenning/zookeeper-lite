package org.apache.zookeeper.protocol;

public class OpCallResult<T extends Operation.CallRequest, V extends Operation.CallResponse> extends OpResult<T,V> implements Operation.CallResult<T,V> {

    public static <T extends Operation.CallRequest, V extends Operation.CallResponse> Operation.CallResult<T,V> create(T request, V response) {
        return new OpCallResult<T,V>(request, response);
    }
    
    public OpCallResult(T request, V response) {
        super(request, response);
    }
    
    @Override
    public long zxid() {
        return response().zxid();
    }

    @Override
    public int xid() {
        return request().xid();
    }
}
