package org.apache.zookeeper.protocol;


public class OpCallResult extends OpResult implements Operation.CallResult {

    public static Operation.CallResult create(Operation.CallRequest request, Operation.CallResponse response) {
        return new OpCallResult(request, response);
    }
    
    public OpCallResult(Operation.CallRequest request, Operation.CallResponse response) {
        super(request, response);
    }
    

    @Override
    public Operation.CallRequest request() {
        return (Operation.CallRequest)request;
    }

    @Override
    public Operation.CallResponse response() {
        return (Operation.CallResponse)response;
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
