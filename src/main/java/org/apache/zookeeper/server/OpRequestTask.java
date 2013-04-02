package org.apache.zookeeper.server;

import java.util.concurrent.Callable;

import org.apache.zookeeper.protocol.Operation;
import org.apache.zookeeper.protocol.Operations;

public class OpRequestTask implements Callable<Operation.Response>, Operation.RequestValue {

    public static OpRequestTask create(Operation.Request request) {
        return new OpRequestTask(request);
    }
    
    protected Operation.Request request;
    
    public OpRequestTask(Operation.Request request) {
        this.request = request;
    }
    
    @Override
    public Operation.Response call() throws Exception {
        return Operations.Responses.create(request().operation());
    }

    @Override
    public Operation operation() {
        return request.operation();
    }

    @Override
    public Operation.Request request() {
        return request;
    }
}
