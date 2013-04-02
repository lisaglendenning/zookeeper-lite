package org.apache.zookeeper.server;

import java.util.concurrent.Callable;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.protocol.OpAction;
import org.apache.zookeeper.protocol.OpCallResult;
import org.apache.zookeeper.protocol.OpError;
import org.apache.zookeeper.protocol.OpResult;
import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;

public class OpResultTask implements Callable<Operation.Result>, Operation.RequestValue {

    public static OpResultTask create(OpCallResponseTask request) {
        return new OpResultTask(request);
    }
    
    protected OpCallResponseTask request;
    
    public OpResultTask(OpCallResponseTask request) {
        this.request = request;
    }
    
    @Override
    public Operation.Result call() throws Exception {
        Operation.Result result;
        Operation.Response response = request.call();
        if (request() instanceof Operation.CallRequest && response instanceof Operation.CallResponse) {
            result = OpCallResult.create((Operation.CallRequest)request(), 
                    (Operation.CallResponse) response);
        } else {
            result = OpResult.create(request(), response);
        }
        return result;
    }

    @Override
    public Operation operation() {
        return request.operation();
    }

    @Override
    public Operation.Request request() {
        return request.request();
    }
}
